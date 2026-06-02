package main

import (
	"fmt"
	"module20_2_1/logger"
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
		logger.Log("pipelineStageFilterNegative", "Старт горутины")
		
		for {
			select {
			case num, ok := <-in:
				if !ok {
					logger.Log("pipelineStageFilterNegative", "Входной канал закрыт")
					return
				}
				logger.Log("pipelineStageFilterNegative", fmt.Sprintf("Получено число: %d", num) )
				
				if num >= 0 {
					logger.Log("pipelineStageFilterNegative", fmt.Sprintf("Число %d прошло фильтр", num))
					select {
					case out <- num:
						logger.Log("pipelineStageFilterNegative", fmt.Sprintf("Число %d отправлено", num))
					case <-stopWorkChannel:
						logger.Log("pipelineStageFilterNegative", "Сигнал остановки")
						return
					}
				} else {
					logger.Log("pipelineStageFilterNegative", fmt.Sprintf("Число %d отфильтровано", num))
				}
			case <-stopWorkChannel:
				logger.Log("pipelineStageFilterNegative", "Сигнал остановки")
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
		logger.Log("pipelineStageNotMultiple", "Старт горутины")
		
		for {
			select {
			case num, ok := <-in:
				if !ok {
					logger.Log("pipelineStageNotMultiple", "Входной канал закрыт")
					return
				}
				logger.Log("pipelineStageNotMultiple", fmt.Sprintf("Получено число: %d", num))

				if num != 0 && num%3 == 0 {
					logger.Log("pipelineStageNotMultiple", fmt.Sprintf("Число %d кратно 3", num))
					select {
					case out <- num:
						logger.Log("pipelineStageNotMultiple", fmt.Sprintf("Число %d отправлено", num))
					case <-stopWorkChannel:
						logger.Log("pipelineStageNotMultiple", "Сигнал остановки")
						return
					}
				} else {
					logger.Log("pipelineStageNotMultiple", fmt.Sprintf("Число %d отфильтровано", num))
				}
			case <-stopWorkChannel:
				logger.Log("pipelineStageNotMultiple", "Сигнал остановки")
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

	logger.Log("buffer", "Создан буфер на 60 элементов")

	go func() {
		logger.Log("buffer", "Старт записи")
		for {
			select {
			case num, ok := <-in:
				if !ok {
					logger.Log("buffer", "Входной канал закрыт")
					return
				}
				logger.Log("buffer", fmt.Sprintf("Добавлено %d", num))
				rb.Add(num)
			case <-stopWorkChannel:
				logger.Log("buffer", "Сигнал остановки")
				return
			}
		}
	}()

	go func() {
		defer close(out)
		logger.Log("buffer", fmt.Sprintf("Старт чтения, таймер %d сек", bufferTimeoutSecond))
		for {
			select {
			case <-time.After(time.Duration(bufferTimeoutSecond) * time.Second):
				if rb.GetCount() > 0 {
					out <- rb.Release()
					logger.Log("buffer", "Извлекли значение из буфера")
				}
			case <-stopWorkChannel:
						logger.Log("buffer", "Сигнал остановки")
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
		logger.Log("terminalScanner", "Старт сканирования")

		var data string

		for {
			select {
			case <-stopWork:
				logger.Log("terminalScanner", "Сигнал остановки")
				return
			default:
			}

			time.Sleep(200 * time.Millisecond)
			fmt.Println("Введите число или exit для выхода: ")

			_, err := fmt.Scanln(&data)
			if err != nil {
				logger.Log("terminalScanner",  fmt.Sprintf("Ошибка ввода: %v", err))
				continue
			}

			logger.Log("terminalScanner", fmt.Sprintf("Введено: %s", data))

			if strings.EqualFold(data, "exit") {
				logger.Log("terminalScanner", "Команда exit")
				close(stopWork)
				return
			}

			i, err := strconv.Atoi(data)
			if err != nil {
				logger.Log("terminalScanner", "Введено не корректное значение")
				fmt.Println("Программа обрабатывает только целые числа!")
				continue
			}
			
			logger.Log("terminalScanner",  fmt.Sprintf("Число %d отправлено", i))
			select {
			case c <- i:
				logger.Log("terminalScanner",  fmt.Sprintf("Число %d в канале", i))
			case <-stopWork:
				logger.Log("terminalScanner", "Сигнал остановки")
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
		logger.Log("createPipeline", "Создали пайплайн")
		return &pipeline{ppStages: pipelineStages, stopWork: stopWorkChannel}
}

func (p *pipeline) runPipeline(data <-chan int) <-chan int {
	resChan := data
	logger.Log("runPipeline", "Запуск")

	for i := range p.ppStages {
		logger.Log("runPipeline", "Запуск стадии очередной стадии")
		resChan = p.runStage(p.ppStages[i], resChan)
	}

	return resChan
}

func (p *pipeline) runStage(stage pipelineStageInterface, sourceData <-chan int) <-chan int {
	return stage.processData(p.stopWork, sourceData)
}

func main() {
	logger.Log("main", "Программа запущена")

	stopWorkChannel := make(chan bool)

	filterNegative := &pipelineStageFilterNegative{}
	filterNotMultiple := &pipelineStageNotMultiple{}
	bufferStage := &buffer{}
	terminalScanner := &terminalScanner{}
	// dataMaker := new(dataMaker)
	// dataChannel := dataMaker.createData(stopWorkChannel)

	ppl := createPipeline(stopWorkChannel, filterNegative, filterNotMultiple, bufferStage)

	// result := ppl.runPipeline(dataChannel)
	result := ppl.runPipeline(terminalScanner.startScan(stopWorkChannel))

		logger.Log("main", "Ожидание результатов")
	for r := range result {
		fmt.Println("Мы прошли все стадии пайплайна:", r)
	}

		logger.Log("main", "Завершено")

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
