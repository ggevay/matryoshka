#!/bin/bash


cat $1 |grep "${rtmarker}" |sed "s/.*${rtmarker}\(.*\)/\1/" |tr -d '\n'
