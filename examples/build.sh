filename=testfile
inputdir=path_to_testfile/
outputdir=path_to_output/
k=6
m=2
dn=20
bs=67108864
mem=8

# 4k 4096
# 1M 1048576
# 4M 4194304
# 16M 16777216
# 64M 67108864
# 128M 134217728
# 256M 268435456

go build -o main ./main.go

./main -md init -k $k -m $m -dn $dn -bs $bs -mem $mem -sn $sn
./main -md encode -f $inputdir/$filename -conStripes 100 -o
# ./main -md psrap -fmd diskFail -fn 1 -f $inputdir/$filename -sl $sl
# ./main -md psras -fmd diskFail -fn 1 -f $inputdir/$filename -sl $sl
# ./main -md psrpa -fmd diskFail -fn 1 -f $inputdir/$filename -sl $sl

# to read a file
./main -md read -f $filename -conStripes 100 -sp ../../output/$filename

srchash=(`sha256sum $inputdir/$filename|tr ' ' ' '`)
dsthash=(`sha256sum $outputdir/$filename|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
