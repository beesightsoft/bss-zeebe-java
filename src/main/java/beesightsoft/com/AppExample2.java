package beesightsoft.com;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.response.DeploymentEvent;
import io.zeebe.client.api.response.WorkflowInstanceEvent;
import io.zeebe.client.api.worker.JobWorker;

public class AppExample2 {
    public static void main(String[] args) {
        final ZeebeClient client = ZeebeClient.newClientBuilder()
                // change the contact point if needed
                .brokerContactPoint("192.168.1.235:26500")
                .usePlaintext()
                .build();

        System.out.println("Connected.");

//        deployWorkflow(client);

        /**
         * Create a workflow with following variables:
         * "orderId": 31243
         * "orderItems": [435, 182, 376]
         * Job type collect-money => take input and output price = 30
         * Job type fetch-items => take input orderItems and output new array with item name as itemId-ready as shipItems
         * Job type ship-parcel => take input shipItems and done
         */
        createWorkflowInstance(client);
        collectMoneyJobWorker(client);
        fetchItemsJobWorker(client);
        shipParcelJobWorker(client);

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
        final Map<String, Object> data = new HashMap<>();
        data.put("orderId", 31243);
        data.put("orderItems", Arrays.asList(435, 182, 376));

        final WorkflowInstanceEvent wfInstance = client.newCreateInstanceCommand()
                .bpmnProcessId("order-process")
                .latestVersion()
                .variables(data)
                .send()
                .join();

        final long workflowInstanceKey = wfInstance.getWorkflowInstanceKey();

        System.out.println("Workflow instance created. Key: " + workflowInstanceKey);
    }

    /**
     * Fetch items job
     */
    private static void collectMoneyJobWorker(ZeebeClient client) {
        String jobType = "collect-money";
        System.out.println("Job worker: " + jobType);
        // after the workflow instance is created
        final JobWorker jobWorker = client.newWorker()
                .jobType(jobType)
                .handler((jobClient, job) ->
                {
                    final Map<String, Object> variables = job.getVariablesAsMap();

                    System.out.println("Process order: " + variables.get("orderId"));
                    List<Integer> orderItems = (List<Integer>) variables.get("orderItems");
                    double price = 30 * orderItems.size();
                    System.out.println("Collect money: $" + price);

                    // Make output
                    final Map<String, Object> result = new HashMap<>();
                    result.put("totalPrice", price);

                    jobClient.newCompleteCommand(job.getKey())
                            .variables(result)
                            .send()
                            .join();
                })
                .fetchVariables("orderId", "orderItems")
                .open();

        // waiting for the jobs

        // Don't close, we need to keep polling to get work
        // jobWorker.close();
    }

    /**
     * Fetch items job
     */
    private static void fetchItemsJobWorker(ZeebeClient client) {
        String jobType = "fetch-items";
        System.out.println("Job worker: " + jobType);
        // after the workflow instance is created
        final JobWorker jobWorker = client.newWorker()
                .jobType(jobType)
                .handler((jobClient, job) ->
                {
                    final Map<String, Object> variables = job.getVariablesAsMap();

                    List<Integer> orderItems = (List<Integer>) variables.get("orderItems");
                    System.out.println("Process items: " + orderItems);
                    List<String> shipItems = new ArrayList<>();
                    for (Integer orderItem : orderItems) {
                        shipItems.add("Item: " + orderItem);
                    }
                    System.out.println("Ship items: " + shipItems.toString());

                    // Make output
                    final Map<String, Object> result = new HashMap<>();
                    result.put("shipItems", shipItems);

                    jobClient.newCompleteCommand(job.getKey())
                            .variables(result)
                            .send()
                            .join();
                })
                .fetchVariables("orderItems")
                .open();

        // waiting for the jobs

        // Don't close, we need to keep polling to get work
        // jobWorker.close();
    }


    /**
     * Ship parcel job
     */
    private static void shipParcelJobWorker(ZeebeClient client) {
        String jobType = "ship-parcel";
        System.out.println("Job worker: " + jobType);
        // after the workflow instance is created
        final JobWorker jobWorker = client.newWorker()
                .jobType(jobType)
                .handler((jobClient, job) ->
                {
                    final Map<String, Object> variables = job.getVariablesAsMap();

                    List<Integer> orderItems = (List<Integer>) variables.get("shipItems");
                    System.out.println("Process on shipItems: " + orderItems);

                    // Make output
                    jobClient.newCompleteCommand(job.getKey())
                            .send()
                            .join();
                })
                .fetchVariables("shipItems")
                .open();

        // waiting for the jobs

        // Don't close, we need to keep polling to get work
        // jobWorker.close();
    }
}