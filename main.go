package main

import (
	"fmt"
	"sync"
	"time"
)

type TaskData struct {
	id  int
	url string
}

func main() {
	// The number of concurrently running go routines 
	// It probably makes sense to start by matching the number of utilizable
	// threads on the system then tune after measuring the performance.
	const worker_count = 16;

	// The maximum number of tasks that the buffered channel (fixed size FIFO
	// queue) can hold before the next attempt to add results in the adder
	// becoming blocked. When a vacancy in the channel occurs, the blocked adder
	// may proceed and the task enters the channel.
	const max_queued_tasks = 32

	// Use this to keep track of how many workers are alive and allow the main
	// thread to wait until all have completed.
	var wait_group sync.WaitGroup

	// create a buffered channel (fixed size FIFO queue) of TaskData objects
	task_channel := make(chan TaskData, max_queued_tasks)

	fmt.Printf("Creating a pool of %d workers listening to a buffered task channel of up to %d tasks at a time\n",
            worker_count, max_queued_tasks)
	for i := 0; i < worker_count; i++ {
		wait_group.Add(1) // register the worker
		worker_id := i    // give the worker a unique id as a captured variable

		// Launch the worker (thread) and have it run the func until the channel is
		// closed and the tasks in the channel run out.
		go func() {
			defer wait_group.Done()
			fmt.Printf("Starting worker %d\n", worker_id)

			// Repeatedly do one of the following:
			//   - get the next available task from the channel
			//   - or block until a task is added if it was empty
			//   - or break from the loop and consequently end the goroutine
			for task := range task_channel {
				fmt.Printf("Worker %d processing task %+v\n", worker_id, task)

				// Do some serious "work" to stress the CPU and force go to use more cores!
				sum := 0;
				for k := 0; k < 10000000000; k++ {
					sum += 1;
				}
				fmt.Printf("sum %d\n", sum);
			}
		}()
	}

	// Now let's create some tasks and send them to the workers
	for i := 0; i < 1000000; i++ {
		task := TaskData{
			id: i,
			url: "http://just_some_url.iguess",
		}
		fmt.Printf("Adding task %+v\n", task)

		// This will block if the buffered channel is full. We must wait until one
		// or more workers take some of the tasks from the channel so that a
		// vacancy appears and we have somewhere to put these new tasks
		task_channel <- task
	}

	// close the channel so the workers know there is no more work to be waited
	// on if the channel runs dry
	close(task_channel)

	// Force the function to block until all workers have indicated they are
	// finished (by calling .Done())
	wait_group.Wait()
	fmt.Println("All done.")
}
