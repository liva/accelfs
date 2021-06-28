#!/bin/bash -xe
#TIME="TIME=true"
#GDB="/opt/nec/ve/bin/gdb --args"
#if [ "${GDB}" == "" ]; then
#  OUTPUT=">>output"
#fi

SCRIPT_DIR="../../scripts"

date
date > output
    echo ">>>$TARGET" >> output
    for BSIZE in 32760 #100 32761
    do
	rm -f vefs vefs_recheck unvme redirect
	docker run --rm -it -v $PWD:$PWD -w $PWD vefs:develop sh -c "make BSIZE=$BSIZE"

	if test 1 -eq 1 ; then
	    echo ">>>${BSIZE}" >> output

        ${SCRIPT_DIR}/cleanup_nvme.sh
	
        ${SCRIPT_DIR}/setup_uio_nvme.sh
        sudo sh -c "${TIME} ${GDB} ./vefs ${OUTPUT}"
        ${SCRIPT_DIR}/cleanup_uio_nvme.sh
            
        ${SCRIPT_DIR}/setup_uio_nvme.sh
		sudo sh -c "${TIME} ${GDB} ./vefs_recheck ${OUTPUT}"
        ${SCRIPT_DIR}/cleanup_uio_nvme.sh
	fi


	if test 1 -eq 0 ; then
	    rm -rf /home/sawamoto/ssd_mem
	    dd if=/dev/zero of=/home/sawamoto/ssd_mem bs=1G count=10
	    sudo sh -c "${TIME} ${GDB} ./vefs ${OUTPUT}"
	    sudo sh -c "${TIME} ${GDB} ./vefs_recheck ${OUTPUT}"
	    rm -rf /home/sawamoto/ssd_mem
	fi
    done
