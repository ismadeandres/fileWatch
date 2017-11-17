package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/mangalaman93/tail"
)

type task struct {
	filePattern string
	thresholds  []threshold
	frequency   int
}

type threshold struct {
	thname  string
	value   string
	message string
}

type config []tsk

type tsk struct {
	Pattern    string `json:"pattern"`
	Frequency  string `json:"frequency"`
	Separator  string `json:"separator"`
	Thresholds []struct {
		Column   string   `json:"column"`
		Message  string   `json:"message"`
		Operator []string `json:"operator"`
		Value    string   `json:"value"`
	} `json:"thresholds"`
}

func main() {

	configuration := parseConfig()
	fmt.Println(configuration)
	tasks := make(chan tsk)
	evaluation := make(chan []threshold)

	go func() {
		//for i := 0; i < 10; i++ {
		fmt.Printf("len config: %v\n", len(configuration))
		for i := 0; i < len(configuration); i++ {
			tasks <- tsk{
				Pattern:    configuration[i].Pattern,
				Frequency:  configuration[i].Frequency,
				Thresholds: configuration[i].Thresholds,
			}
		}
		close(tasks)
	}()
	var wg sync.WaitGroup
	wg.Add(len(configuration))

	for i := 0; i < len(configuration); i++ {
		go func() {
			monitor(<-tasks, evaluation)
		}()

		defer wg.Done()
	}

	for e := range evaluation {
		if len(e) > 0 {
			fmt.Printf("there are some result on the channel\n")
		}
	}

	wg.Wait()

}

func monitor(t tsk, evaluation chan []threshold) error {
	fmt.Printf("Monitoring task: %v\n", t.Frequency)
	duration, err := time.ParseDuration(t.Frequency)
	if err != nil {
		errors.Wrap(err, "could not parse duration for "+t.Pattern)
	}

	c := time.Tick(duration)

	for _ = range c {

		fmt.Printf("reading file %s every %s\n", t.Pattern, t.Frequency)
		for thr := range t.Thresholds {
			fmt.Printf("therssobhold: %s -> %v\n", t.Thresholds[thr].Column, t.Thresholds[thr].Value)
		}
		t := threshold{thname: ""}
		ths := []threshold{}
		ths = append(ths, t)
		//th := threshold{thname: "asdasasa"}
		readCSV("ejieSinks/director-1-1-A_ejie_LKVPLEX40_com_stats.csv", ths)

		evaluation <- ths
	}
	return nil
	//read fields
	//identify thresholds on fields
	//check thresholds

}

func readCSV(file string, thresholds []threshold) ([]string, error) {
	tail,err:= tail.TailFile(file,1000,15); if err!=nil{
		fmt.Errorf("could not tail file %s: %s", file, err)
	}else{
		fmt.Printf("file ok tailored!\n")
	}
	defer tail.Stop()
	for l :=range <-tail.Lines{
		fmt.Printf("%v --------------------------\n",l)
}
	f, err := os.Open(file)
	if err != nil {
		fmt.Errorf("there is some error openning file: %s [%s]\n", file, err)
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.Comma = ','
	header, err := csvr.Read()
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		fmt.Errorf("there is an error scanning contents of %s [ %s ]\n", file, err)
		return []string{"adada"}, err
	}
	fmt.Printf("header: %v\n", header[1])

	for h := range header {
		for thr := range thresholds {
			fmt.Printf("therssobhold: %s -> %v\n", thresholds[thr].thname, thresholds[thr].value)
		}
		fmt.Printf("h: %i\n", h)
	}

	return []string{header[1]}, nil
	/*
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			//fmt.Println(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			fmt.Errorf("there is an error scanning contents of %s [ %s ]\n", file, err)
		}
		return []string{"terwr"}, nil
	*/
}

func parseConfig() config {
	c := config{}

	fname := "fileWatcher.cfg"
	file, err := os.Open(fname)
	if err != nil {
		errors.Wrap(err, "could not open configuration file\n")
	}
	decoder := json.NewDecoder(file)

	err = decoder.Decode(&c)
	if err != nil {
		errors.Wrap(err, "could not decode configuration file\n")
		panic(err)
	}
	fmt.Println(c)

	return c
}

type operator struct{}
