package test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	pkgLog "github.com/kapitanov/natandb/pkg/log"
	"github.com/kapitanov/natandb/pkg/proto"

	"github.com/spf13/cobra"
	"gonum.org/v1/gonum/stat"
)

var log = pkgLog.New("")

// Command is root for test commands
var Command = &cobra.Command{
	Use:              "test",
	Short:            "Test tools",
	Hidden:           true,
	TraverseChildren: true,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Printf("try \"%s %s --help\" for more information\n", cmd.CommandPath(), cmd.Use)
		os.Exit(0)
	},
}

type perfTest interface {
	Name() string
	Init(ctx context.Context, client proto.Client, n int) error
	Run(ctx context.Context, client proto.Client, low, high int) error
	Cleanup(ctx context.Context, client proto.Client, n int) error
}

func testCmd(cmd *cobra.Command, test perfTest) {
	endpoint := cmd.Flags().StringP("endpoint", "e", "127.0.0.1:18081", "server endpoint")
	iterations := cmd.Flags().IntP("iter", "i", 10, "num of iterations")
	n := cmd.Flags().IntP("num", "n", 10000, "num of operations per iteration")
	t := cmd.Flags().IntP("threads", "t", 1, "num of concurrent threads")

	cmd.Run = func(c *cobra.Command, args []string) {
		log.Printf("connecting to %s...", *endpoint)
		client, err := proto.NewClient(*endpoint)
		if err != nil {
			log.Printf("unable to connect: %s", err)
			panic(err)
		}

		defer func() {
			err = client.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		go func() {
			for s := range signals {
				if s == os.Interrupt {
					cancel()
				}
			}
		}()

		numIterations := *iterations
		fmt.Printf("TEST:              \"%s\"\n", test.Name())
		fmt.Printf("ITERATIONS:        %d\n", numIterations)
		fmt.Printf("OPS PER ITERATION: %d\n", *n)
		fmt.Printf("CONCURRENT OPS:    %d\n", *t)

		rates := make([]float64, 0)
		for iter := 0; iter < numIterations; iter++ {
			fmt.Printf("ITERATION %2d/%d: init", iter+1, numIterations)
			err := test.Init(ctx, client, *n)
			if err != nil {
				panic(err)
			}

			fmt.Printf("\rITERATION %2d/%d: exec", iter+1, numIterations)
			duration, rate := execTest(ctx, test, client, *n, *t)

			fmt.Printf("\rITERATION %2d/%d: clean", iter+1, numIterations)
			err = test.Cleanup(ctx, client, *n)
			if err != nil {
				panic(err)
			}

			rates = append(rates, rate)
			fmt.Printf("\rITERATION %2d/%d: total time %0.3f s\trate %0.1f rps\n", iter+1, numIterations, duration.Seconds(), rate)			
		}

		weights:=make([]float64, len(rates))
		for i := range  rates {
			weights[i] = 1
		}

		mean,std := stat.MeanStdDev(rates, weights)
		fmt.Printf("MEAN RATE %0.1f rps\n",mean)		
		fmt.Printf("STD DEV %0.1f\n",std)		
	}
}

func execTest(ctx context.Context, test perfTest, client proto.Client, n, t int) (duration time.Duration, rate float64) {
	startTime := time.Now()

	c := make(chan error)
	for i := 0; i < t; i++ {
		low := i * n / t
		high := (i + 1) * n / t
		go func() {
			err := test.Run(ctx, client, low, high)
			c <- err
		}()
	}

	for i := 0; i < t; i++ {
		err := <-c
		if err != nil {
			panic(err)
		}
	}

	duration = time.Now().Sub(startTime)
	rate = float64(n) / duration.Seconds()
	return
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
