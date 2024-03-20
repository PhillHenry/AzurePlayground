package uk.co.odinconsultants.azure;

import com.azure.identity.DefaultAzureCredential;
import com.azure.resourcemanager.datafactory.DataFactoryManager;
import com.azure.resourcemanager.datafactory.models.Factory;
import com.azure.resourcemanager.storage.StorageManager;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.Region;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Configuration;
import com.azure.core.util.CoreUtils;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.datafactory.models.AzureBlobDataset;
import com.azure.resourcemanager.datafactory.models.AzureStorageLinkedService;
import com.azure.resourcemanager.datafactory.models.BlobSink;
import com.azure.resourcemanager.datafactory.models.BlobSource;
import com.azure.resourcemanager.datafactory.models.CopyActivity;
import com.azure.resourcemanager.datafactory.models.CreateRunResponse;
import com.azure.resourcemanager.datafactory.models.DatasetReference;
import com.azure.resourcemanager.datafactory.models.Factory;
import com.azure.resourcemanager.datafactory.models.LinkedServiceReference;
import com.azure.resourcemanager.datafactory.models.PipelineResource;
import com.azure.resourcemanager.datafactory.models.PipelineRun;
import com.azure.resourcemanager.datafactory.models.TextFormat;
import com.azure.resourcemanager.storage.StorageManager;
import com.azure.resourcemanager.storage.models.PublicAccess;
import com.azure.resourcemanager.storage.models.StorageAccount;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static java.lang.String.format;

public class ExampleMain {
    private static final Logger logger = LoggerFactory.getLogger(ExampleMain.class);


    private static final Region REGION = Region.UK_SOUTH;
    private static final String DATA_FACTORY = "udal-adf-data-dmarts-prod";

    public static void main(String[] args) {

        var tenantId = args[0];
        var subscriptionId = args[1];
        var resourceGroup = args[2];
        var factoryId = args[3];

        System.out.println(format("tenantId      : %-50s", tenantId));
        System.out.println(format("subscriptionId: %-50s", subscriptionId));
        System.out.println(format("factoryId     : %-50s", factoryId));

        DefaultAzureCredential credentials = new DefaultAzureCredentialBuilder().tenantId(tenantId).build();
        DataFactoryManager manager =  DataFactoryManager
                .configure().withLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BASIC))
                .authenticate(credentials, new AzureProfile(tenantId, subscriptionId, AzureEnvironment.AZURE));


// data factory
        for (Factory factory: manager.factories().listByResourceGroup(resourceGroup)) {
            System.out.println(factory);
        }
        Factory dataFactory = manager.factories().getById(factoryId).refresh();
/*
// linked service
        StorageManager storageManager = null;

// container
        final String containerName = "adf";


        final Map<String, String> connectionStringProperty = new HashMap<>();
        connectionStringProperty.put("type", "SecureString");
        connectionStringProperty.put("value", connectionString);



        final String linkedServiceName = "LinkedService";
        manager.linkedServices().define(linkedServiceName)
                .withExistingFactory(resourceGroup, DATA_FACTORY)
                .withProperties(new AzureStorageLinkedService()
                        .withConnectionString(connectionStringProperty))
                .create();

// input dataset
        final String inputDatasetName = "InputDataset";
        manager.datasets().define(inputDatasetName)
                .withExistingFactory(resourceGroup, DATA_FACTORY)
                .withProperties(new AzureBlobDataset()
                        .withLinkedServiceName(new LinkedServiceReference().withReferenceName(linkedServiceName))
                        .withFolderPath(containerName)
                        .withFileName("input/data.txt")
                        .withFormat(new TextFormat()))
                .create();

// output dataset
        final String outputDatasetName = "OutputDataset";
        manager.datasets().define(outputDatasetName)
                .withExistingFactory(resourceGroup, DATA_FACTORY)
                .withProperties(new AzureBlobDataset()
                        .withLinkedServiceName(new LinkedServiceReference().withReferenceName(linkedServiceName))
                        .withFolderPath(containerName)
                        .withFileName("output/data.txt")
                        .withFormat(new TextFormat()))
                .create();

// pipeline
        PipelineResource pipeline = manager.pipelines().define("CopyBlobPipeline")
                .withExistingFactory(resourceGroup, DATA_FACTORY)
                .withActivities(Collections.singletonList(new CopyActivity()
                        .withName("CopyBlob")
                        .withSource(new BlobSource())
                        .withSink(new BlobSink())
                        .withInputs(Collections.singletonList(new DatasetReference().withReferenceName(inputDatasetName)))
                        .withOutputs(Collections.singletonList(new DatasetReference().withReferenceName(outputDatasetName)))))
                .create();

// run pipeline
        CreateRunResponse createRun = pipeline.createRun();

// wait for completion
        PipelineRun pipelineRun = manager.pipelineRuns().get(resourceGroup, DATA_FACTORY, createRun.runId());
        String runStatus = pipelineRun.status();
        while ("InProgress".equals(runStatus)) {
            sleepIfRunningAgainstService(10 * 1000);    // wait 10 seconds
            pipelineRun = manager.pipelineRuns().get(resourceGroup, DATA_FACTORY, createRun.runId());
            runStatus = pipelineRun.status();
        }
*/
    }
}
