#!/bin/bash

grep -Eo "Game ([[:digit:]]+)|([[:digit:]]*) (red|green|blue)" 2023/day2.txt | awk '
BEGIN {part_1_answer = 0; part_2_answer = 0}
$1 == "Game" {
    if(green_over == 0 && red_over == 0 && blue_over == 0) part_1_answer += game; 
    part_2_answer += (green_max * red_max * blue_max); 
    game = $2; 
    green_over=0; green_max=0;
    red_over=0; red_max=0;
    blue_over=0; blue_max=0;
};
$2 == "red" {if ($1 > 12) red_over=$1; if (red_max < $1) red_max = $1};
$2 == "green" {if ($1 > 13) green_over=$1; if (green_max < $1) green_max = $1};
$2 == "blue" {if ($1 > 14) blue_over=$1; if (blue_max < $1) blue_max = $1};
END {
    if(green_over == 0 && red_over == 0 && blue_over == 0) part_1_answer += game;
    part_2_answer += (green_max * red_max * blue_max); 
    print "Part 1:", part_1_answer;
    print "Part 2:", part_2_answer;
};
'