﻿// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Newtonsoft.Json;
using DataX.Config.ConfigDataModel;
using DataX.Config.ConfigDataModel.RuntimeConfig;
using DataX.Config.Utility;
using DataX.Contract;
using DataX.Flow.CodegenRules;
using System;
using System.Collections.Generic;
using System.Composition;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataX.Config.ConfigGeneration.Processor
{
    /// <summary>
    /// Produce the outputs section
    /// </summary>
    [Shared]
    [Export(typeof(IFlowDeploymentProcessor))]
    public class ResolveOutputs : ProcessorBase
    {
        public const string TokenName_Outputs = "outputs";


        [ImportingConstructor]
        public ResolveOutputs(ConfigGenConfiguration configuration, IKeyVaultClient keyVaultClient)
        {
            Configuration = configuration;
            KeyVaultClient = keyVaultClient;
            RuntimeKeyVaultName = new Lazy<string>(() => Configuration[Constants.ConfigSettingName_RuntimeKeyVaultName], true);
        }

        private ConfigGenConfiguration Configuration { get; }
        private IKeyVaultClient KeyVaultClient { get; }
        protected Lazy<string> RuntimeKeyVaultName { get; }

        public override int GetOrder()
        {
            return 500;
        }

        public override async Task<string> Process(FlowDeploymentSession flowToDeploy)
        {
            var config = flowToDeploy.Config;
            var guiConfig = config.GetGuiConfig();

            if (guiConfig == null)
            {
                return "no gui input, skipped";
            }

            var rulesCode = flowToDeploy.GetAttachment<RulesCode>(GenerateTransformFile.AttachmentName_CodeGenObject);
            Ensure.NotNull(rulesCode, "rulesCode");
            Ensure.NotNull(rulesCode.MetricsRoot, "rulesCode.MetricsRoot");
            Ensure.NotNull(rulesCode.MetricsRoot.metrics, "rulesCode.MetricsRoot.metrics");

            var outputs = await ProcessOutputs(guiConfig.Outputs, rulesCode, config.Name);

            flowToDeploy.SetObjectToken(TokenName_Outputs, outputs);

            return "done";
        }

        public override async Task<FlowGuiConfig> HandleSensitiveData(FlowGuiConfig guiConfig)
        {           
            var outputsData = guiConfig?.Outputs;
            if (outputsData != null && outputsData.Length > 0)
            {
                foreach (var rd in outputsData)
                {
                    var connStr = rd.Properties?.ConnectionString;
                    if (connStr != null && !KeyVaultUri.IsSecretUri(connStr))
                    {
                        var secretName = $"{guiConfig.Name}-output";
                        var secretUri = await KeyVaultClient.SaveSecretAsync(
                            keyvaultName: RuntimeKeyVaultName.Value,
                            secretName: secretName,
                            secretValue: connStr,
                            hashSuffix: true);

                        rd.Properties.ConnectionString = secretUri;
                    }
                }
            }

            return guiConfig;
        }


        private async Task<FlowOutputSpec[]> ProcessOutputs(FlowGuiOutput[] uiOutputs, RulesCode rulesCode, string configName)
        {
            var outputList = uiOutputs.Select(o => o.Id).ToList();

            var outputsFiltered = rulesCode.Outputs.Where(o => outputList.Contains(o.Item2)).ToList();

            var flattenedOutputs = new Dictionary<string, List<string>>();
            foreach (var o in outputsFiltered)
            {
                var outputNames = o.Item1.Split(new char[] { ',' });
                foreach (string outputName in outputNames)
                {
                    var name = outputName.Trim();
                    if (flattenedOutputs.TryGetValue(name, out List<string> val))
                    {
                        val.Add(o.Item2);
                    }
                    else
                    {
                        flattenedOutputs[name] = new List<string>() { o.Item2 };
                    }
                }
            }

            List<FlowOutputSpec> fOutputList = new List<FlowOutputSpec>();

            foreach (var fOut in flattenedOutputs)
            {
                FlowOutputSpec flowOutput = new FlowOutputSpec() { Name = fOut.Key };

                foreach (var item in fOut.Value)
                {
                    var output = uiOutputs.SingleOrDefault(o => o.Id.Equals(item));
                    switch (output.Type.ToLower())
                    {
                        case "cosmosdb":
                            {
                                var cosmosDbOutput = ProcessOutputCosmosDb(output);
                                Ensure.EnsureNullElseThrowNotSupported(flowOutput.CosmosDbOutput, "Multiple target cosmosDB ouptut for same dataset not supported.");
                                flowOutput.CosmosDbOutput = cosmosDbOutput;
                                break;
                            }
                        case "eventhub":
                            {
                                var eventhubOutput = ProcessOutputEventHub(output);
                                Ensure.EnsureNullElseThrowNotSupported(flowOutput.EventHubOutput, "Multiple target eventHub/metric ouptut for same dataset not supported.");
                                flowOutput.EventHubOutput = eventhubOutput;
                                break;
                            }
                        case "metric":
                            {
                                if (LocalUtility.IsLocalEnabled(Configuration))
                                {
                                    if (Configuration.TryGet(Constants.ConfigSettingName_LocalMetricsHttpEndpoint, out string localMetricsEndpoint))
                                    {
                                        var httpOutput = ProcessLocalOutputMetric(configName, localMetricsEndpoint);
                                        Ensure.EnsureNullElseThrowNotSupported(flowOutput.HttpOutput, "Multiple target httpost/metric ouptut for same dataset not supported.");
                                        flowOutput.HttpOutput = httpOutput;                                       
                                    }
                                    break;
                                }
                                else
                                {
                                    var eventhubOutput = ProcessOutputMetric(output);
                                    Ensure.EnsureNullElseThrowNotSupported(flowOutput.EventHubOutput, "Multiple target eventHub/metric ouptut for same dataset not supported.");
                                    flowOutput.EventHubOutput = eventhubOutput;
                                    break;
                                }
                            }
                        case "blob":
                            {
                                var blobOutput = await ProcessOutputBlob(configName, output);
                                Ensure.EnsureNullElseThrowNotSupported(flowOutput.BlobOutput, "Multiple target blob ouptut for same dataset not supported.");
                                flowOutput.BlobOutput = blobOutput;
                                break;
                            }
                        case "local":
                            {
                                var blobOutput = ProcessOutputLocal(configName, output);
                                Ensure.EnsureNullElseThrowNotSupported(flowOutput.BlobOutput, "Multiple target blob ouptut for same dataset not supported.");
                                flowOutput.BlobOutput = blobOutput;
                                break;
                            }
                        default:
                            throw new NotSupportedException($"{output.Type} output type not supported");
                    }
                }

                fOutputList.Add(flowOutput);
            }

            return fOutputList.ToArray();
        }

        private async Task<FlowBlobOutputSpec> ProcessOutputBlob(string configName, FlowGuiOutput uiOutput)
        {
            if (uiOutput != null && uiOutput.Properties != null)
            {
                var sparkKeyVaultName = Configuration[Constants.ConfigSettingName_RuntimeKeyVaultName];

                string connectionString = await KeyVaultClient.ResolveSecretUriAsync(uiOutput.Properties.ConnectionString);
                var accountName = ParseBlobAccountName(connectionString);
                var blobPath = $"wasbs://{uiOutput.Properties.ContainerName}@{accountName}.blob.core.windows.net/{uiOutput.Properties.BlobPrefix}/%1$tY/%1$tm/%1$td/%1$tH/${{quarterBucket}}/${{minuteBucket}}";
                var secretId = $"{configName}-output";
                var blobPathSecret = await KeyVaultClient.SaveSecretAsync(sparkKeyVaultName, secretId, blobPath, true);
                await KeyVaultClient.SaveSecretAsync(sparkKeyVaultName, $"datax-sa-{accountName}", ParseBlobAccountKey(connectionString), false);

                FlowBlobOutputSpec blobOutput = new FlowBlobOutputSpec()
                {
                    CompressionType = uiOutput.Properties.CompressionType,
                    Format = uiOutput.Properties.Format,
                    Groups = new BlobOutputGroups()
                    {
                        Main = new BlobOutputMain()
                        {
                            Folder = blobPathSecret
                        }
                    }
                };
                return blobOutput;
            }
            else
            {
                return null;
            }
        }

        private FlowCosmosDbOutputSpec ProcessOutputCosmosDb(FlowGuiOutput uiOutput)
        {
            if (uiOutput != null && uiOutput.Properties != null)
            {
                FlowCosmosDbOutputSpec cosmoDbOutput = new FlowCosmosDbOutputSpec()
                {
                    ConnectionStringRef = uiOutput.Properties.ConnectionString,
                    Database = uiOutput.Properties.Db,
                    Collection = uiOutput.Properties.Collection
                };
                return cosmoDbOutput;
            }
            else
            {
                return null;
            }
        }

        private FlowEventHubOutputSpec ProcessOutputMetric(FlowGuiOutput uiOutput)
        {
            if (uiOutput != null && uiOutput.Properties != null)
            {
                var sparkKeyVaultName = Configuration[Constants.ConfigSettingName_RuntimeKeyVaultName];
                var metricsEhConnectionStringKey = Configuration[Constants.ConfigSettingName_MetricEventHubConnectionKey];

                FlowEventHubOutputSpec eventhubOutput = new FlowEventHubOutputSpec()
                {
                    ConnectionStringRef = KeyVaultUri.ComposeUri(sparkKeyVaultName, metricsEhConnectionStringKey),
                    CompressionType = "none",
                    Format = "json"
                };
                return eventhubOutput;
            }
            else
            {
                return null;
            }
        }

        private FlowEventHubOutputSpec ProcessOutputEventHub(FlowGuiOutput uiOutput)
        {
            if (uiOutput != null && uiOutput.Properties != null)
            {
                FlowEventHubOutputSpec eventhubOutput = new FlowEventHubOutputSpec()
                {
                    ConnectionStringRef = uiOutput.Properties.ConnectionString,
                    CompressionType = uiOutput.Properties.CompressionType,
                    Format = uiOutput.Properties.Format
                };
                return eventhubOutput;
            }
            else
            {
                return null;
            }
        }

        private FlowBlobOutputSpec ProcessOutputLocal(string configName, FlowGuiOutput uiOutput)
        {
            if (uiOutput != null && uiOutput.Properties != null)
            {
                string outputPath = uiOutput.Properties.ConnectionString.TrimEnd('/');
                var localPath = $"{outputPath}/%1$tY/%1$tm/%1$td/%1$tH/${{quarterBucket}}/${{minuteBucket}}";

                FlowBlobOutputSpec localOutput = new FlowBlobOutputSpec()
                {
                    CompressionType = uiOutput.Properties.CompressionType,
                    Format = uiOutput.Properties.Format,
                    Groups = new BlobOutputGroups()
                    {
                        Main = new BlobOutputMain()
                        {
                            Folder = localPath
                        }
                    }
                };
                return localOutput;
            }
            else
            {
                return null;
            }
        }

        private FlowHttpOutputSpec ProcessLocalOutputMetric(string configName, string endPoint)
        {
            Dictionary<string, string> headers = new Dictionary<string, string>()
            {
                { "Content-Type", "application/json" },
                { "jobName", configName}
             };

            return new FlowHttpOutputSpec()
            {
                Endpoint = endPoint,
                Filter = "",
                Headers = headers
            };

        }

        /// <summary>
        /// Parses the account name from connection string
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns>account name</returns>
        private string ParseBlobAccountName(string connectionString)
        {
            string matched;
            try
            {
                matched = Regex.Match(connectionString, @"(?<=AccountName=)(.*)(?=;AccountKey)").Value;
            }
            catch (Exception)
            {
                return "The connectionString does not have AccountName";
            }

            return matched;
        }

        /// <summary>
        /// Parses the account key from connection string
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns>account key</returns>
        private string ParseBlobAccountKey(string connectionString)
        {
            string matched;
            try
            {
                matched = Regex.Match(connectionString, @"(?<=AccountKey=)(.*)(?=;EndpointSuffix)").Value;
            }
            catch (Exception)
            {
                return "The connectionString does not have AccountKey";
            }

            return matched;
        }
    }
}
