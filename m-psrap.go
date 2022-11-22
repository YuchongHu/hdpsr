package hdpsr

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) PartialStripeMultiRecoverPreliminary(
	fileName string, slowLatency int, options *Options) (
	map[string]string, error) {
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

	baseName := filepath.Base(fileName)

	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	diskFailList := make(map[int]bool, failNum)
	j := e.DiskNum
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].mntPath] =
				e.diskInfos[j].mntPath
			replaceMap[i] = j
			diskFailList[i] = true
			j++
		}
	}

	// open all disks
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)
	// alive := int32(0)
	for i, disk := range e.diskInfos[0:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.mntPath, baseName)
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
			folderPath := filepath.Join(disk.mntPath, baseName)
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
	err = e.getDiskBWRead(ifs)
	if err != nil {
		return nil, err
	}
	intraStripe := e.getIntraStripeOptimal(slowLatency)
	// t := time.Since(start).Seconds()
	// fmt.Println("mpsrap algorithm running time: ", t)
	// logfile := "log.txt"
	// file, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	// if err != nil && err != io.EOF {
	// 	panic(err)
	// }
	// str := fmt.Sprintf("mpsrap algorithm running time: %f\n", t)
	// file.Write([]byte(str))
	// file.Close()

	// fmt.Println("intraStripe: ", intraStripe)

	// read stripes every blob in parallel
	// read blocks every stripe in parallel
	stripeNum, stripes := e.getStripes(diskFailList)
	if !e.Quiet {
		log.Printf("Start recovering with stripe, totally %d stripes need recovery",
			stripeNum)
	}
	e.ConStripes = (e.MemSize * GiB) / (intraStripe * int(e.BlockSize))
	e.ConStripes = minInt(e.ConStripes, stripeNum)
	if e.ConStripes == 0 {
		return nil, errors.New("no stripes to be recovered or memory size is too small")
	}
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr3DByte(e.ConStripes, intraStripe, int(e.BlockSize))
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
				// fmt.Printf("stripe %d dist: %v\n", spId, dist)
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
				// get the latency array
				stripeToDiskArr := make([]*sortNode, 0)
				fail := 0
				for i := 0; i < e.K; i++ {
					diskId := dist[i+fail]
					if !e.diskInfos[diskId].available {
						fail += 1
						i -= 1
						continue
					}
					stripeToDiskArr = append(stripeToDiskArr, &sortNode{diskId: diskId, idx: i, blockId: i + fail, latency: e.diskInfos[diskId].latency})
				}
				for len(stripeToDiskArr) > 0 {
					group := BiggestK(stripeToDiskArr, intraStripe)
					for i := range group {
						i := i
						diskId := group[i].diskId
						blockId := group[i].blockId
						erg.Go(func() error {
							offset := blockToOffset[blockId]
							_, err := ifs[diskId].ReadAt(blobBuf[s][i][0:e.BlockSize],
								int64(offset)*e.BlockSize)
							if err != nil && err != io.EOF {
								return err
							}
							return nil
						})
					}
					if err = erg.Wait(); err != nil {
						return err
					}
					inputsIdx := make([]int, 0)
					for i := range group {
						inputsIdx = append(inputsIdx, int(group[i].idx))
					}
					tempShard, err = e.enc.MultiRecoverWithSomeShards(decodeMatrix, blobBuf[s][:len(group)], inputsIdx, invalidIndices, tempShard)
					if err != nil {
						return err
					}
					// delete visited disk in stripeToDiskArr
					if intraStripe > len(stripeToDiskArr) {
						stripeToDiskArr = stripeToDiskArr[len(stripeToDiskArr):]
					} else {
						stripeToDiskArr = stripeToDiskArr[intraStripe:]
					}
				}
				// for j := 0; j < len(tempShard); j++ {
				// 	fmt.Printf("stripe %d tempShard %d: %v\n", spId, j, string(tempShard[j]))
				// }
				// write the block to backup disk
				egp := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(egp)

				orderMap := make(map[int]int)
				tmp := 0
				for j := 0; j < len(invalidIndices); j++ {
					orderMap[dist[invalidIndices[j]]] = tmp
					tmp++
				}
				for i := 0; i < e.K+e.M; i++ {
					diskId := dist[i]
					v := 0
					ok := false
					if v, ok = replaceMap[diskId]; ok {
						diskId := diskId
						restoreId := v - e.DiskNum
						writeOffset := blockToOffset[i]
						egp.Go(func() error {
							restoreId := restoreId
							writeOffset := writeOffset
							diskId := diskId
							tmpId := orderMap[diskId]
							// fmt.Printf("stripe %d disk %d tmpId: %v\n", spId, diskId, tmpId)
							_, err := rfs[restoreId].WriteAt(tempShard[tmpId], int64(writeOffset)*e.BlockSize)
							if err != nil {
								return err
							}
							if e.diskInfos[diskId].ifMetaExist {
								newMetapath := filepath.Join(e.diskInfos[restoreId].mntPath, "META")
								if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
									return err
								}
							}
							return nil
						})
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

	// err = e.updateDiskPath(replaceMap)
	// if err != nil {
	// 	return nil, err
	// }
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}
