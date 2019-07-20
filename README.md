# External Sorting (Mergesort)

External sorting implementation, using ScyllaDB's library SeaStar.

## Abstract

Sorting file much bigger than memory available requires special algorithm, 
loading to memory that much data it may contain, without overfilling it, not
to use swap.

Thereby I present my implementation of external merge sort, using asynchronous,
super-efficient library, based on features and promises (thus IO locks reduced).

## Build

TODO
