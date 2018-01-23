#!/usr/bin/python
#coding=utf8

import os,re

def irq():
    #return irq number and network interface number
    #exp:
    #irq iface
    #61  0
    #62  1
    cpunum = os.popen("cat /proc/cpuinfo|grep \"model name\"|wc -l").read().replace("\n","")
    r = os.popen("cat /proc/interrupts |grep -E \"eth[0-9]-\"|awk '{sub(\"eth[0-9]-\",\"\",$%s);print $1,$%s}'"%(int(cpunum)+3,int(cpunum)+3)).readlines()
    return [ (i.split()[0].split(":")[0],re.sub("[a-zA-Z]", "",i.replace("\n","").split()[1])) for i in r ]

def main(irq_queuenum):

    # if exists irqbalance process,will killed"
    os.popen("/etc/init.d/irq_balancer stop")
    irqbalance = int(os.popen("ps axu|grep irqbalance|grep -v grep|wc -l").read())
    if irqbalance > 0:os.popen("pkill irqbalance");print "irqbalance is kill"

    # set irq_affinity
    for i in irq_queuenum:
        set_irq_affinity(i[0],hex(1 << int(i[1])).replace('0x',''))

def set_irq_affinity(IRQ,MASK):
    print 'echo %s to /proc/irq/%s/smp_affinity'%(MASK,IRQ)
    fp = open('/proc/irq/%s/smp_affinity'%IRQ,'w')
    fp.write(str(MASK))
    fp.close()

main(irq())