#!/bin/sh

svn up
ant clean
ant
ant regression-complete
