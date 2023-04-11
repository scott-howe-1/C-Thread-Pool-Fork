#! /bin/bash

#
# This file has several functional tests similar to what a user
# might use in his/her code
#

. funcs.sh


function test_mass_addition { #endsum #threads #useresults
	echo "Adding up to $1 with $2 threads with result flag $3"
	compile src/conc_increment.c
	output=$(./test $1 $2 $3)
	sum_threads=$(echo $output | awk '{print $(NF)}')
	if [ "$sum_threads" != "$1" ]; then
		err "Expected $1 from threads but got $output" "$output"
		exit 1
	fi
	if [ $3 != 0 ]; then
		sum_results=$(echo $output | awk '{print $(NF-1)}')
		if [ "$sum_results" != "$1" ]; then
			err "Expected $1 from results but got $output" "$output"
			exit 1
		fi
	fi
}


# Run tests
test_mass_addition 100 4 0
test_mass_addition 100 1000 0
test_mass_addition 100000 1000 0
test_mass_addition 100 4 1
test_mass_addition 100 1000 1
test_mass_addition 100000 1000 1

echo "No errors"
