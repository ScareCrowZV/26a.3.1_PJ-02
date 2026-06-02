package main

import (
	"fmt"
	ringBuffer "module20_2_1_ringBufferInt"
	"strconv"
	"strings"
	"time"
)

var bufferTimeoutSecond int

// type dataMaker struct {
// }

// func (dm *dataMaker) createData(stopWorkChannel chan bool) <-chan int {
// 	data := make(chan int)

// 	go func() {
// 		defer close(data)
// 		for i := range 10 {
// 			select {
// 			case data <- i:
// 			case <-stopWorkChannel:
// 				return
// 			}
// 		}

// 	}()

// 	return data
// }

type pipelineStageInterface interface {
	processData(stopWorkChannel <-chan bool, in <-chan int) <-chan int
}

type pipelineStageFilterNegative struct {
}

func (p *pipelineStageFilterNegative) processData(stopWorkChannel <-chan bool, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for {
			select {
			case num, ok := <-in:
				if !ok {
					return
				}
				if num >= 0 {
					select {
					case out <- num:
					case <-stopWorkChannel:
						return
					}

				}
			case <-stopWorkChannel:
				return
			}
		}
	}()

	return out
}

type pipelineStageNotMultiple struct {
}

func (p *pipelineStageNotMultiple) processData(stopWorkChannel <-chan bool, in <-chan int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for {
			select {
			case num := <-in:
				if num != 0 && num%3 == 0 {
					select {
					case out <- num:
					case <-stopWorkChannel:
						return
					}

				}
			case <-stopWorkChannel:
				return
			}
		}
	}()

	return out
}

type buffer struct {
}

func (b *buffer) processData(stopWorkChannel <-chan bool, in <-chan int) <-chan int {
	out := make(chan int)
	rb := ringBuffer.CreateRingBuffer(60)

	go func() {
		defer close(out)
		for {
			select {
			case num := <-in:
				rb.Add(num)
			case <-stopWorkChannel:
				return
			}
		}
	}()

	go func() {
		defer close(out)
		for {
			select {
			case <-time.After(time.Duration(bufferTimeoutSecond) * time.Second):
				if rb.GetCount() > 0 {
					out <- rb.Release()
				}
			case <-stopWorkChannel:
				return
			}
		}
	}()

	return out
}

type terminalScanner struct {
}

func (ts *terminalScanner) startScan(stopWork chan bool) <-chan int {
	c := make(chan int)

	go func() {
		defer close(c)
		var data string

		for {
			select {
			case <-stopWork:
				return
			default:
			}

			time.Sleep(200 * time.Millisecond)
			fmt.Println("Введите число или exit для выхода: ")

			_, err := fmt.Scanln(&data)
			if err != nil {
				continue
			}

			if strings.EqualFold(data, "exit") {
				close(stopWork)
				return
			}

			i, err := strconv.Atoi(data)
			if err != nil {
				fmt.Println("Программа обрабатывает только целые числа!")
				continue
			}

			select {
			case c <- i:
			case <-stopWork:
				return
			}
		}
	}()

	return c
}

type pipeline struct {
	ppStages []pipelineStageInterface
	stopWork <-chan bool
}

func createPipeline(stopWorkChannel <-chan bool, pipelineStages ...pipelineStageInterface) *pipeline {
	return &pipeline{ppStages: pipelineStages, stopWork: stopWorkChannel}
}

func (p *pipeline) runPipeline(data <-chan int) <-chan int {
	resChan := data

	for i := range p.ppStages {
		resChan = p.runStage(p.ppStages[i], resChan)
	}

	return resChan
}

func (p *pipeline) runStage(stage pipelineStageInterface, sourceData <-chan int) <-chan int {
	return stage.processData(p.stopWork, sourceData)
}

func main() {

	stopWorkChannel := make(chan bool)

	filterNegative := new(pipelineStageFilterNegative)
	filterNotMultiple := new(pipelineStageNotMultiple)
	bufferStage := new(buffer)
	terminalScanner := new(terminalScanner)
	// dataMaker := new(dataMaker)
	// dataChannel := dataMaker.createData(stopWorkChannel)

	ppl := createPipeline(stopWorkChannel, filterNegative, filterNotMultiple, bufferStage)

	// result := ppl.runPipeline(dataChannel)
	result := ppl.runPipeline(terminalScanner.startScan(stopWorkChannel))

	for r := range result {
		fmt.Println("Мы прошли все стадии пайплайна:", r)
	}

	// rb := createRingBuffer(30)

	// var cur, nx *ringCell
	// for range 200 {
	// 	if cur == nil {
	// 		cur = rb.read
	// 		nx = cur.next
	// 	}

	// 	fmt.Println(cur)
	// 	cur = nx
	// 	nx = cur.next

	// }

	// rb := ringBuf.CreateRingBuffer(30)

	// for i := range 30 {
	// 	i += 100
	// 	rb.Add(i)
	// }

	// for range 30 {
	// 	fmt.Println(rb.Release())
	// }

	// dataMaker := func() <-chan int {
	// 	data := make(chan int)
	// 	go func() {
	// 		for i := range 10 {
	// 			data <- i
	// 		}
	// 	}()
	// 	return data
	// }

	// dataOutputer := func(data <-chan int) {
	// 	go func() {
	// 		for {
	// 			select {
	// 			case d := <-data:
	// 				fmt.Println("Получены данные: ", d)
	// 			}
	// 		}
	// 	}()
	// }

	// data := dataMaker()
	// dataOutputer(data)
	// time.Sleep(1 * time.Second)
}
