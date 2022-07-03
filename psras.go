package hdpsr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) getIntraStripeFast() int {
	intraStripe := 0
	for _, stripe := range e.Stripes {
		cnt := 0
		for _, diskId := range stripe.Dist {
			if e.diskInfos[diskId].slow {
				cnt++
			}
		}
		intraStripe = max(intraStripe, cnt)
	}
	if intraStripe > e.K/2 {
		intraStripe = e.K / 2
	} else if intraStripe < 2 {
		intraStripe = 2
	}
	return intraStripe
}

func (e *Erasure) PartialStripeRecoverSlowerFirst(fileName string, slowLatency int, options *Options) (map[string]string, error) {
	var failDisk int = 0
	for i := range e.diskInfos {
		if !e.diskInfos[i].available {
			failDisk = i
			break
		}
	}
	if !e.Quiet {
		log.Printf("Start recovering with stripe, totally %d stripes need recovery",
			len(e.StripeInDisk[failDisk]))
	}
	if err != nil {
		return nil, err
	}
	baseName := filepath.Base(fileName)
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)

	ReplaceMap[e.diskInfos[failDisk].diskPath] = e.diskInfos[e.DiskNum].diskPath
	replaceMap[failDisk] = e.DiskNum

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
				fmt.Println("!")
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
	disk := e.diskInfos[e.DiskNum]
	// fmt.Println(disk.diskPath)
	folderPath := filepath.Join(disk.diskPath, baseName)
	blobPath := filepath.Join(folderPath, "BLOB")
	if e.Override {
		if err := os.RemoveAll(folderPath); err != nil {
			return nil, err
		}
	}
	if err := os.Mkdir(folderPath, 0777); err != nil {
		return nil, errDataDirExist
	}
	rfs, err := os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return nil, err
	}
	defer rfs.Close()

	start := time.Now()
	intraStripe := e.getIntraStripeFast()
	t := time.Since(start).Seconds()
	// fmt.Println("psras algorithm running time: ", t)
	logfile := "log.txt"
	file, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	str := fmt.Sprintf("    psras algorithm running time: %f\n", t)
	file.Write([]byte(str))
	file.Close()

	start = time.Now()
	// read stripes every blob in parallel
	// read blocks every stripe in parallel
	stripeNum := len(e.StripeInDisk[failDisk])
	e.ConStripes = (e.MemSize * 1024 * 1024 * 1024) / (intraStripe * int(e.BlockSize))
	e.ConStripes = min(e.ConStripes, stripeNum)
	if e.ConStripes == 0 {
		return nil, errors.New("memory size is too small")
	}
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr3DByte(e.ConStripes, intraStripe, int(e.BlockSize))
	stripeCnt := 0
	nextStripe := 0
	stripes := e.StripeInDisk[failDisk]

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
				tempShard := make([]byte, e.BlockSize)
				// get decodeMatrix of each stripe
				invalidIndice := -1
				for i, blk := range dist {
					if blk == failDisk {
						invalidIndice = i
						break
					}
				}
				invalidIndices := []int{invalidIndice}
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
					tempShard, err = e.enc.RecoverWithSomeShards(decodeMatrix, blobBuf[s][:len(group)], inputsIdx, invalidIndice, tempShard)
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
				// write the block to backup disk
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					if diskId == failDisk {
						writeOffset := blockToOffset[i]
						_, err := rfs.WriteAt(tempShard, int64(writeOffset)*e.BlockSize)
						if err != nil {
							return err
						}
						if e.diskInfos[diskId].ifMetaExist {
							newMetapath := filepath.Join(e.diskInfos[e.DiskNum].diskPath, "META")
							if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
								return err
							}
						}
						break
					}
				}
				// fmt.Printf("stripe %d: transfer time: %v, total time: %v, ratio of transfer: %v\n", spId, transferTime, totalTime, transferTime/(totalTime+t))
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
