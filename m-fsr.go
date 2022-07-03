package hdpsr

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) FullStripeMultiRecover(fileName string, slowLatency int, options *Options) (map[string]string, error) {
	// start1 := time.Now()
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

	// fmt.Println("first phase costs: ", time.Since(start1).Seconds())

	// start2 := time.Now()
	// read stripes every blob in parallel
	// read blocks every stripe in parallel
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
	blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
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
				// s := s
				spId := stripes[stripeNo]
				spInfo := e.Stripes[spId]
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get dist and blockToOffset by stripeNo
				dist := spInfo.Dist
				blockToOffset := spInfo.BlockToOffset
				// fmt.Println(spId, dist, blockToOffset)
				// read blocks in parallel
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					disk := e.diskInfos[diskId]
					if !disk.available {
						continue
					}
					erg.Go(func() error {
						offset := blockToOffset[i]
						_, err := ifs[diskId].ReadAt(blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
							int64(offset)*e.BlockSize)
						if err != nil && err != io.EOF {
							return err
						}
						return nil
					})
				}
				if err := erg.Wait(); err != nil {
					return err
				}
				//Split the blob into k+m parts
				splitData, err := e.splitStripe(blobBuf[s])
				if err != nil {
					return err
				}
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					err = e.enc.ReconstructWithList(splitData, &diskFailList, &(dist), options.Degrade)
					if err != nil {
						return err
					}
				}
				//write the Blob to restore paths
				egp := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(egp)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					if v, ok := replaceMap[diskId]; ok {
						restoreId := v - e.DiskNum
						writeOffset := blockToOffset[i]
						egp.Go(func() error {
							_, err := rfs[restoreId].WriteAt(splitData[i], int64(writeOffset)*e.BlockSize)
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

	// fmt.Println("second phase costs: ", time.Since(start2).Seconds())

	// start3 := time.Now()
	err = e.updateDiskPath(replaceMap)
	// fmt.Println("third phase costs: ", time.Since(start3).Seconds())
	if err != nil {
		return nil, err
	}
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}

func (e *Erasure) getStripes(diskFailList map[int]bool) (int, []int64) {
	stripes := make([]int64, 0)
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			stripes = append(stripes, e.StripeInDisk[i]...)
		}
	}
	stripes = removeRepByMap(stripes)
	return len(stripes), stripes
}

func removeRepByMap(slc []int64) []int64 {
	result := []int64{}
	tempMap := map[int64]byte{}
	for _, e := range slc {
		l := len(tempMap)
		tempMap[e] = 0

		if len(tempMap) != l {
			result = append(result, e)
		}
	}
	return result
}
