package swarm

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	jtypes "github.com/ffrank/jaas/pkg/types"

	"github.com/davecgh/go-spew/spew"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

func RunTask(taskRequest jtypes.TaskRequest) error {
	if validationErr := validate(taskRequest); validationErr != nil {
		return validationErr
	}

	if taskRequest.Verbose {
		fmt.Printf("Running.. OK %s\n", taskRequest.Image)
		fmt.Printf("Connected to.. OK %s\n", taskRequest.Networks)
		fmt.Printf("Constraints: %s\n", taskRequest.Constraints)
		fmt.Printf("envVars: %s\n", taskRequest.EnvVars)
		fmt.Printf("Secrets: %s\n", taskRequest.Secrets)
	}

	timeoutVal, parseErr := time.ParseDuration(taskRequest.Timeout)
	if parseErr != nil {
		return parseErr
	}

	if taskRequest.Verbose {
		fmt.Printf("timeout: %s\n", timeoutVal)
	}

	var c *client.Client
	var err error
	c, err = client.NewEnvClient()
	if err != nil {

		return fmt.Errorf("is the Docker Daemon running? Error: %s", err.Error())
	}

	// Check that experimental mode is enabled on the daemon, fall back to no logging if not
	versionInfo, versionErr := c.ServerVersion(context.Background())
	if versionErr != nil {
		log.Fatal("Is the Docker Daemon running?")

		return versionErr
	}

	if taskRequest.ShowLogs {
		apiVersion, parseErr := strconv.ParseFloat(versionInfo.APIVersion, 64)
		if parseErr != nil {
			apiVersion = 0
		}
		if apiVersion < 1.29 && versionInfo.Experimental == false {
			return fmt.Errorf("experimental daemon or Docker API Version 1.29+ required to display service logs, falling back to no log display")
		}
	}

	spec := makeSpec(taskRequest.Image, taskRequest.EnvVars)
	if len(taskRequest.Networks) > 0 {
		nets := []swarm.NetworkAttachmentConfig{
			swarm.NetworkAttachmentConfig{Target: taskRequest.Networks[0]},
		}
		spec.Networks = nets
	}

	createOptions := types.ServiceCreateOptions{}

	if len(taskRequest.RegistryAuth) > 0 {
		createOptions.EncodedRegistryAuth = taskRequest.RegistryAuth
		createOptions.QueryRegistry = true
		fmt.Println("Using RegistryAuth")
	}

	placement := &swarm.Placement{}
	if len(taskRequest.Constraints) > 0 {
		placement.Constraints = taskRequest.Constraints
		spec.TaskTemplate.Placement = placement
	}

	if len(taskRequest.Command) > 0 {
		spec.TaskTemplate.ContainerSpec.Command = strings.Split(taskRequest.Command, " ")
	}

	if len(taskRequest.EnvFiles) > 0 {
		for _, file := range taskRequest.EnvFiles {
			envs, err := readEnvs(file)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err)
				os.Exit(1)
			}

			for _, env := range envs {
				spec.TaskTemplate.ContainerSpec.Env = append(spec.TaskTemplate.ContainerSpec.Env, env)
			}
		}
	}

	spec.TaskTemplate.ContainerSpec.Mounts = []mount.Mount{}
	for _, bindMount := range taskRequest.Mounts {
		parts := strings.Split(bindMount, "=")
		if len(parts) < 2 || len(parts) > 2 {
			fmt.Fprintf(os.Stderr, "Bind-mounts must be specified as: src=dest, i.e. --mount /home/alex/tmp/=/tmp/\n")
			os.Exit(1)
		}

		if len(parts) == 2 {
			mountVal := mount.Mount{
				Source: parts[0],
				Target: parts[1],
			}

			spec.TaskTemplate.ContainerSpec.Mounts = append(spec.TaskTemplate.ContainerSpec.Mounts, mountVal)
		}
	}

	secretList, err := c.SecretList(context.Background(), types.SecretListOptions{})

	spec.TaskTemplate.ContainerSpec.Secrets = []*swarm.SecretReference{}
	for _, serviceSecret := range taskRequest.Secrets {
		var secretID string
		for _, s := range secretList {
			if serviceSecret == s.Spec.Annotations.Name {
				secretID = s.ID
				break
			}
		}
		if secretID == "" {
			fmt.Fprintf(os.Stderr, "No existing secret has name that matches %s\n", serviceSecret)
			os.Exit(1)
		}

		secretVal := swarm.SecretReference{
			File: &swarm.SecretReferenceFileTarget{
				Name: serviceSecret,
				UID:  "0",
				GID:  "0",
				Mode: os.FileMode(0444), // File can be read by any user inside the container
			},
			SecretName: serviceSecret,
			SecretID:   secretID,
		}

		spec.TaskTemplate.ContainerSpec.Secrets = append(spec.TaskTemplate.ContainerSpec.Secrets, &secretVal)
	}

	if taskRequest.Verbose {
		fmt.Printf("Creating service with this spec:\n\t%v\nOptions:\n\t%+v\n", spew.Sdump(spec), createOptions)
	}

	createResponse, err := c.ServiceCreate(context.Background(), spec, createOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating service: %v\n", err)
		os.Exit(1)
	}
	opts := types.ServiceInspectOptions{InsertDefaults: true}

	service, _, err := c.ServiceInspectWithRaw(context.Background(), createResponse.ID, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error querying service details for %v: %v\n", createResponse.ID, err)
		os.Exit(1)
	}
	fmt.Printf("Service created: %s (%s)\n", service.Spec.Name, createResponse.ID)
	fmt.Printf("Warnings:\n%v\n", spew.Sdump(createResponse.Warnings))

	taskExitCode := pollTask(c, createResponse.ID, timeoutVal, taskRequest.ShowLogs, taskRequest.RemoveService)
	os.Exit(taskExitCode)
	return nil
}

func makeSpec(image string, envVars []string) swarm.ServiceSpec {
	max := uint64(1)

	spec := swarm.ServiceSpec{
		TaskTemplate: swarm.TaskSpec{
			RestartPolicy: &swarm.RestartPolicy{
				MaxAttempts: &max,
				Condition:   swarm.RestartPolicyConditionNone,
			},
			ContainerSpec: &swarm.ContainerSpec{
				Image: image,
				Env:   envVars,
			},
		},
	}
	return spec
}

func readEnvs(file string) ([]string, error) {
	var err error
	var envs []string

	data, readErr := ioutil.ReadFile(file)
	if readErr != nil {
		return envs, readErr
	}

	lines := strings.Split(string(data), "\n")
	for n, line := range lines {
		if len(line) > 0 {
			if strings.Index(line, "=") == -1 {
				err = fmt.Errorf("no seperator found in line %d of env-file %s", n, file)
				break
			}
			envs = append(envs, line)
		}
	}
	return envs, err
}

const swarmError = -999
const timeoutError = -998

func pollTask(c *client.Client, id string, timeout time.Duration, showlogs, removeService bool) int {
	svcFilters := filters.NewArgs()
	svcFilters.Add("id", id)

	exitCode := swarmError

	opts := types.ServiceListOptions{
		Filters: svcFilters,
	}

	list, _ := c.ServiceList(context.Background(), opts)
	for _, item := range list {
		start := time.Now()
		end := start.Add(timeout)

		fmt.Println("ID: ", item.ID, " Update at: ", item.UpdatedAt)
		for {
			time.Sleep(500 * time.Millisecond)

			taskExitCode, found := showTasks(c, item.ID, showlogs, removeService)
			if found {
				exitCode = taskExitCode
				break
			}
			now := time.Now()
			if now.After(end) {
				fmt.Printf("Timing out after %s.", timeout.String())
				return timeoutError
			}
		}
	}

	return exitCode
}

func showTasks(c *client.Client, id string, showLogs, removeService bool) (int, bool) {
	filters1 := filters.NewArgs()
	filters1.Add("service", id)

	tasks, _ := c.TaskList(context.Background(), types.TaskListOptions{
		Filters: filters1,
	})

	exitCode := 1
	var done bool
	stopStates := []swarm.TaskState{
		swarm.TaskStateComplete,
		swarm.TaskStateFailed,
		swarm.TaskStateRejected,
	}

	for _, task := range tasks {

		stop := false
		for _, stopState := range stopStates {
			if task.Status.State == stopState {
				stop = true
				break
			}
		}

		if stop {
			fmt.Printf("\n\n")
			fmt.Printf("Exit code: %d\n", task.Status.ContainerStatus.ExitCode)
			fmt.Printf("State: %s\n", task.Status.State)
			fmt.Printf("\n\n")

			exitCode = task.Status.ContainerStatus.ExitCode

			if exitCode == 0 && task.Status.State == swarm.TaskStateRejected {
				exitCode = 255 // force non-zero exit for task rejected
			}

			if showLogs {
				fmt.Println("Printing service logs")
			}

			if showLogs {
				logRequest, err := c.ServiceLogs(context.Background(), id, types.ContainerLogsOptions{
					Follow:     false,
					ShowStdout: true,
					ShowStderr: true,
					Timestamps: true,
					Details:    false,
					Tail:       "all",
				})

				if err != nil {
					fmt.Printf("Unable to pull service logs.\nError: %s\n", err)
				} else {
					defer logRequest.Close()

					//	, ShowStderr: true, ShowStdout: true})
					res, _ := ioutil.ReadAll(logRequest)

					fmt.Println(string(res[:]))
				}
			}

			if removeService {
				fmt.Println("Removing service...")
				if err := c.ServiceRemove(context.Background(), id); err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
			}

			done = true
			break
		} else {
			fmt.Printf(".")
		}
	}
	return exitCode, done
}

func validate(taskRequest jtypes.TaskRequest) error {
	if len(taskRequest.Image) == 0 {
		return fmt.Errorf("must a valid supply --image")
	}
	return nil
}
