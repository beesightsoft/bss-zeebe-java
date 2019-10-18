package beesightsoft.com;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.response.DeploymentEvent;
import io.zeebe.client.api.response.WorkflowInstanceEvent;
import io.zeebe.client.api.worker.JobWorker;

// https://docs.zeebe.io/java-client/get-started.html
public class App {
    public static void main(String[] args) {
        final ZeebeClient client = ZeebeClient.newClientBuilder()
                // change the contact point if needed
                .brokerContactPoint("192.168.1.235:26500")
                .usePlaintext()
                .build();

        System.out.println("Connected.");

//        deployWorkflow(client);
//        createWorkflowInstance(client);
        jobWorker(client, "collect-money");
        jobWorker(client, "fetch-items");
        jobWorker(client, "ship-parcel");

//        client.close();
//        System.out.println("Closed.");
    }

    /**
     * Deploy a workflow
     * Output: Workflow deployed. Version: 1
     */
    private static void deployWorkflow(ZeebeClient client) {
        String workFlowFile = "order-process.bpmn";

        // after the client is connected
        System.out.println("Deploy a workflow: " + workFlowFile);
        final DeploymentEvent deployment = client.newDeployCommand()
                .addResourceFromClasspath(workFlowFile)
                .send()
                .join();

        final int version = deployment.getWorkflows().get(0).getVersion();
        System.out.println("Workflow deployed. Version: " + version);
    }

    /**
     * Create a workflow instance
     * Output: Workflow instance created. Key: 2113425532
     */
    private static void createWorkflowInstance(ZeebeClient client) {
        System.out.println("Create a workflow instance");

        // after the workflow is deployed
        final WorkflowInstanceEvent wfInstance = client.newCreateInstanceCommand()
                .bpmnProcessId("order-process")
                .latestVersion()
                .send()
                .join();

        final long workflowInstanceKey = wfInstance.getWorkflowInstanceKey();

        System.out.println("Workflow instance created. Key: " + workflowInstanceKey);
    }

    /**
     * Work on a job after the workflow instance is created
     */
    private static void jobWorker(ZeebeClient client, String jobType) {
        System.out.println("Job worker: " +  jobType);
        // after the workflow instance is created
        final JobWorker jobWorker = client.newWorker()
                .jobType(jobType)
                .handler((jobClient, job) ->
                {
                    System.out.println("Collect money");

                    // ...

                    jobClient.newCompleteCommand(job.getKey())
                            .send()
                            .join();
                })
                .open();

        // waiting for the jobs

        // Don't close, we need to keep polling to get work
        // jobWorker.close();
    }
}