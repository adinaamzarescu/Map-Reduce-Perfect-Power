#!/bin/bash

correct=0
total=0
scalability=0
correctness=0
correct_scalability=0
times=()
seq_times=()
par_times=()
seq_runs=0

function show_score {
	echo ""
	echo "Score scalability: $scalability/70"
	echo "Score correctness: $correctness/50"
	echo "Score:       $((correctness + scalability))/120"
}

function compare_outputs {
	ok=0

	for E in `seq 2 $3`
	do
		diff -q $1/out$E.txt $2/out$E.txt
		if [ $? == 0 ]
		then
			ok=$((ok+1))
		fi
	done

	count=$3
	count=$((count-1))

	if [ $ok == $count ]
	then
		correct=$((correct+1))
	else
		echo "W: Differences between $1 and $2"
		correct_scalability=$((correct_scalability-1))
	fi
}

function run {
	{ time -p sh -c "timeout 120 $1"; } &> time.txt
	ret=$?

	if [ $ret == 124 ]
	then
		echo "W: The program took more than 120 s"
	elif [ $ret != 0 ]
	then
		echo "W: Couldn't execute"
		cat time.txt | sed '$d' | sed '$d' | sed '$d'
	fi

	res=`cat time.txt | grep real | awk '{print $2}'`

	total=$((total+1))
	rm -rf time.txt

	seq_runs=$((seq_runs+1))
}

function run_and_get_time {
	{ time -p sh -c "timeout 120 $1"; } &> time.txt
	ret=$?

	if [ $ret == 124 ]
	then
		echo "W: The program took more than 120 s"
		cat time.txt | sed '$d' | sed '$d' | sed '$d'
		show_score
		rm -rf test*_sec
		exit
	elif [ $ret != 0 ]
	then
		echo "W: Couldn't execute"
		cat time.txt | sed '$d' | sed '$d' | sed '$d'
		show_score
		rm -rf test*_sec
		exit
	fi

	res=`cat time.txt | grep real | awk '{print $2}'`
	seq_times+=(${res})
	times+=(${res%.*})

	total=$((total+1))
	rm -rf time.txt

	seq_runs=$((seq_runs+1))
}

function run_par_and_measure {
	{ time -p sh -c "timeout $1 $2" ; } &> time.txt
	ret=$?

	if [ $ret == 124 ]
	then
		echo "W: The program took at least 2 seconds more than the secvential implementation."
	elif [ $ret != 0 ]
	then
		echo "W: Couldn't execute"
		cat time.txt | sed '$d' | sed '$d' | sed '$d'
	fi

	total=$((total+1))
	par_times+=(`cat time.txt | grep real | awk '{print $2}'`)
	rm -rf time.txt
}

make clean &> /dev/null
make build &> build.txt

if [ ! -f a.out ]
then
	echo "E: Couldn't compile"
	cat build.txt
	show_score
	rm -rf build.txt
	exit
fi

rm -rf build.txt

for i in `seq 0 4`
do
	echo ""
	echo "======== Test ${i} ========"
	echo ""
	echo "Case M=1 si R=4..."

	mkdir -p test${i}_sec

	run_and_get_time "./a.out 1 4 ./test${i}/test.txt > test${i}_sec/out.txt"
	mv out*.txt test${i}_sec/ 2>/dev/null

	echo "It took ${seq_times[$i]} seconds"

	correct_scalability=3

	compare_outputs test${i}_sec test${i} 5

	rm -rf test${i}_sec

	echo "OK"
	echo ""

	times[$i]=$((times[$i]+2))

	mkdir -p test${i}_par

	for P in 2 4
	do
		echo "Case M=$P si R=4..."

		run_par_and_measure ${times[$i]} "./a.out $P 4 ./test${i}/test.txt > test${i}_par/out.txt"
		mv out*.txt test${i}_par/ 2>/dev/null

		if [ $P == 2 ]
		then
			echo "Rularea a durat ${par_times[$i * 2]} secunde"
		else
			echo "Rularea a durat ${par_times[$i * 2 + 1]} secunde"
		fi
		compare_outputs test${i}_par test${i} 5

		echo "OK"
		echo ""
	done

	rm -rf test${i}_par

	if [ $i != 0 ] && [ $i != 4 ]
	then
		speedup12=$(echo "${seq_times[$i]}/${par_times[$i * 2]}" | bc -l | xargs printf "%.2f")
		speedup14=$(echo "${seq_times[$i]}/${par_times[$i * 2 + 1]}" | bc -l | xargs printf "%.2f")

		if [ $correct_scalability != 3 ]
		then
			speedup12=0
			speedup14=0
			echo "Wrong results"
		fi

		printf "Acceleration 1-2 Mappers: %0.2f\n" $speedup12
		printf "Acceleration 1-4 Mappers: %0.2f\n" $speedup14

		if [ $i != 1 ]
		then
			max=$(echo "${speedup12} > 1.4" | bc -l)
			part=$(echo "${speedup12} > 1.1" | bc -l)
			if [ $max == 1 ]
			then
				scalability=$((scalability+14))
			elif [ $part == 1 ]
			then
				scalability=$((scalability+7))
				echo "W: Acceleration from 1 to 2 Mappers too low"
			else
				echo "W: Acceleration from 1 to 2 Mappers too low"
			fi
		fi

		max=$(echo "${speedup14} > 2.1" | bc -l)
		part=$(echo "${speedup14} > 1.8" | bc -l)
		if [ $max == 1 ]
		then
			scalability=$((scalability+14))
		elif [ $part == 1 ]
		then
			scalability=$((scalability+7))
			echo "W: Acceleration from 1 to 4 Mappers too low"
		else
			echo "W: Acceleration from 1 to 4 Mappers too low"
		fi
	fi

	echo "=========================="
	echo ""
done

correctness=$((correct * 50 / total))

make clean &> /dev/null

show_score
