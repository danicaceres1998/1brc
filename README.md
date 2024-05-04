# 1 Billion Row Challenge with *Go*
This is the [1brc](https://github.com/gunnarmorling/1brc) challenge, which was originally devised by Java developers. In this instance, I've chosen to tackle this challenge using Go, as you can see from the title. Below is a guide on how to compile and run this code so that you can test it in your local development environment.

## Objectives
The main objective of completing this challenge was to enhance my knowledge of Go. Given this notion, the challenge itself is interesting as it makes you realize that small details can impact you when working at a large scale.

Upon reviewing the challenge's problem statement itself and also examining the code of other engineers (from whom I drew much inspiration), I made the decision to focus on the following points:

- ***Processing:*** My starting point was ensuring that my algorithm takes less than 30 seconds to complete the entire process.
- ***Readability and maintainability:*** I aimed to make this code as readable and maintainable as possible, as I noticed in other examples that it was very difficult (or took a lot of effort) to understand what was happening underneath.
- ***Utilizing all of Go's concurrency tools:*** goroutines, channels, wait groups,etc.
- Learning more about the ***unsafe*** package (*v1: resulted in a 35% improvement in my code; v2: removed due to memory leaks and unexpected behavior*).
- ***Writing Unit Tests:*** To validate each component of the application, I decided to develop under the TDD philosophy.

## Compiling
`Go version: 1.22.2`

For compile the code run:
``` bash
go build -o bin/1brc main.go
```
For execute the binary:
``` bash
bin/1brc measurements.txt
```
Obs: If you don't have the input data, clone the [1brc](https://github.com/gunnarmorling/1brc) repo and follow the guide to generate the file.

## Results
Best result achieved on my notebook is `8.07s`. My machine is a *MacBook Pro 16' 2019* with:
- *Intel Core i7-9750H CPU @ 2.60GHz, six cores*
- *16 GB 2400 MHz DDR4 RAM*
- *SSD 256 GB*
