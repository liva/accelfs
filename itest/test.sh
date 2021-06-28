#!/bin/sh
rm -f vefs
docker run --rm -it -v $PWD:$PWD -w $PWD vefs:develop sh -c "make BSIZE=100 TARGET=READ"
sudo blkdiscard /dev/nvme0n1
sleep 1

echo '0000:b3:00.0' | sudo tee /sys/bus/pci/drivers/nvme/unbind
echo '0000:b3:00.0' | sudo tee /sys/bus/pci/drivers/uio_pci_generic/bind
sudo ./unvme
echo '0000:b3:00.0' | sudo tee /sys/bus/pci/drivers/uio_pci_generic/unbind
echo '0000:b3:00.0' | sudo tee /sys/bus/pci/drivers/nvme/bind


