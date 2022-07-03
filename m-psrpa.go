package hdpsr

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) PartialStripeMultiRecoverPassive(fileName string, slowLatency int, option *Options) (map[string]string, error) {
	failNum := 0
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			failNum++
		}
	}
	if failNum == 0 {
		return nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNum > e.M {
		return nil, errTooFewDisksAlive
	}
	//the failure number doesn't exceed the fault tolerance
	//but unluckily we don't have enough backups!
	if failNum > len(e.diskInfos)-e.DiskNum {
		return nil, errNotEnoughBackupForRecovery
	}

	// if !e.Quiet {
	// 	log.Printf("Start recovering with stripe, totally %d stripes need recovery",
	// 		len(e.StripeInDisk[failDisk]))
	// }
	baseName := filepath.Base(fileName)
	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	diskFailList := make(map[int]bool, failNum)
	j := e.DiskNum
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].diskPath] = e.diskInfos[j].diskPath
			replaceMap[i] = j
			diskFailList[i] = true
			j++
		}
	}
	// start recovering: recover all stripes in this disk

	// open all disks
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)
	// alive := int32(0)
	for i, disk := range e.diskInfos[0:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				ifs[i] = nil
				return nil
			}
			ifs[i], err = os.Open(blobPath)
			if err != nil {
				return err
			}

			disk.available = true
			// atomic.AddInt32(&alive, 1)
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("read failed %s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < e.DiskNum; i++ {
			if ifs[i] != nil {
				ifs[i].Close()
			}
		}
	}()
	if !e.Quiet {
		log.Println("start reconstructing blocks")
	}
	// create BLOB in the backup disk
	rfs := make([]*os.File, failNum)
	for i := 0; i < failNum; i++ {
		i := i
		disk := e.diskInfos[e.DiskNum+i]
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if e.Override {
				if err := os.RemoveAll(folderPath); err != nil {
					return err
				}
			}
			if err := os.Mkdir(folderPath, 0777); err != nil {
				return errDataDirExist
			}
			rfs[i], err = os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("create BLOB failed %s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < failNum; i++ {
			if rfs[i] != nil {
				rfs[i].Close()
			}
		}
	}()

	// start := time.Now()
	stripeNum, stripes := e.getStripes(diskFailList)
	if !e.Quiet {
		log.Printf("Start recovering with stripe, totally %d stripes need recovery",
			stripeNum)
	}
	e.ConStripes = (e.MemSize * 1024 * 1024 * 1024) / int(e.dataStripeSize)
	e.ConStripes = min(e.ConStripes, stripeNum)
	if e.ConStripes == 0 {
		return nil, errors.New("memory size is too small")
	}
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr3DByte(e.ConStripes, e.K, int(e.BlockSize))
	stripeCnt := 0
	nextStripe := 0

	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			eg.Go(func() error {
				spId := stripes[stripeNo]
				spInfo := e.Stripes[spId]
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get dist and blockToOffset by stripeNo
				dist := spInfo.Dist
				blockToOffset := spInfo.BlockToOffset
				// get decodeMatrix of each stripe
				invalidIndices := make([]int, 0)
				for i, blk := range dist {
					if _, ok := replaceMap[blk]; ok {
						invalidIndices = append(invalidIndices, i)
					}
				}
				tempShard := make([][]byte, len(invalidIndices))
				for i := 0; i < len(invalidIndices); i++ {
					tempShard[i] = make([]byte, e.BlockSize)
				}
				// invalidIndices = append(invalidIndices, invalidIndice)
				decodeMatrix, err := e.enc.GetDecodeMatrix(invalidIndices)
				if err != nil {
					return err
				}
				// if there are slow disks
				fail := 0
				nodeArr := make([][]*node, 0)
				for i := 0; i < 2; i++ {
					nodeArr = append(nodeArr, make([]*node, 0))
				}
				for i := 0; i < e.K; i++ {
					diskId := dist[i+fail]
					if !e.diskInfos[diskId].available {
						fail++
						i--
						continue
					}
					if !e.diskInfos[diskId].slow {
						nodeArr[0] = append(nodeArr[0], &node{diskId: diskId, idx: i, blockId: i + fail})
					} else {
						nodeArr[1] = append(nodeArr[1], &node{diskId: diskId, idx: i, blockId: i + fail})
					}
				}
				sent := 0
				round := 0
				for sent < e.K {
					for i := range nodeArr[round] {
						i := i
						diskId := nodeArr[round][i].diskId
						blockId := nodeArr[round][i].blockId
						timeout := make(chan bool)
						erg.Go(func() error {
							start := time.Now()
							finish := <-timeout
							if finish {
								elapse := time.Since(start)
								if elapse >= time.Second*time.Duration(2) {
									e.diskInfos[diskId].slow = true
								}
							}
							return nil
						})
						erg.Go(func() error {
							offset := blockToOffset[blockId]
							_, err := ifs[diskId].ReadAt(blobBuf[s][i][0:e.BlockSize],
								int64(offset)*e.BlockSize)
							if err != nil && err != io.EOF {
								return err
							}
							timeout <- true
							close(timeout)
							return nil
						})
					}
					if err = erg.Wait(); err != nil {
						return err
					}
					inputsIdx := make([]int, 0)
					for i := range nodeArr[round] {
						inputsIdx = append(inputsIdx, nodeArr[round][i].idx)
					}
					tempShard, err = e.enc.MultiRecoverWithSomeShards(decodeMatrix, blobBuf[s][:len(nodeArr[round])], inputsIdx, invalidIndices, tempShard)
					if err != nil {
						return err
					}
					sent += len(nodeArr[round])
					round++
				}
				// write the block to backup disk
				egp := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(egp)
				tempId := 0
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					v := 0
					ok := false
					tempId := tempId
					if v, ok = replaceMap[diskId]; ok {
						restoreId := v - e.DiskNum
						writeOffset := blockToOffset[i]
						egp.Go(func() error {
							_, err := rfs[restoreId].WriteAt(tempShard[tempId], int64(writeOffset)*e.BlockSize)
							if err != nil {
								return err
							}
							if e.diskInfos[diskId].ifMetaExist {
								newMetapath := filepath.Join(e.diskInfos[restoreId].diskPath, "META")
								if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
									return err
								}
							}
							return nil
						})
					}
					if ok {
						tempId++
					}
				}
				if err := egp.Wait(); err != nil {
					return err
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}
	// fmt.Println("recover time: ", time.Since(start).Seconds())

	err = e.updateDiskPath(replaceMap)
	if err != nil {
		return nil, err
	}
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}
