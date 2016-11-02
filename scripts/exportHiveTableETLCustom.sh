hive -f $1 | /usr/bin/perl -pe 's/((?<=\t)|^)(NULL|NaN)($|(?=\t))/\\N/g' >$2
