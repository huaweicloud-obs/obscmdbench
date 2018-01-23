#!/bin/bash

cd `dirname $0`

UsersFile='users.dat'
>$UsersFile

if [ $# != 2 -o $1 -gt $2 -o 0 -ge $1 ]; then
	echo 'Please input two para'
	echo '$1 is start number, $2 is end number'
	echo '$2 must be equal or greater than $1 and $1 must be greater than 0'
	exit
fi

for((i=$1; i<=$2; i++))
do
	NumStr=`printf "%06d\n" $i`
	AK="UDSIAMSTUBTEST"${NumStr}
	SK=Udsiamstubtest000000$AK
	UserName=TestUser${AK}
	echo $UserName","$AK","$SK"," >>$UsersFile
done

