﻿// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using DataX.Config.ConfigDataModel;
using DataX.Config.Utility;
using DataX.Contract;
using System;
using System.Collections.Generic;
using System.Composition;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DataX.Config.ConfigGeneration.Processor
{
    /// <summary>
    /// Produce the schema file section
    /// </summary>
    [Shared]
    [Export(typeof(IFlowDeploymentProcessor))]
    public class PrepareSchemaFile: ProcessorBase
    {
        public const string TokenName_InputSchemaFilePath = "inputSchemaFilePath";

        [ImportingConstructor]
        public PrepareSchemaFile(IKeyVaultClient keyvaultClient, IRuntimeConfigStorage runtimeStorage, ConfigGenConfiguration conf)
        {
            KeyVaultClient = keyvaultClient;
            RuntimeStorage = runtimeStorage;
            Configuration = conf;
        }

        private ConfigGenConfiguration Configuration { get; }
        private IKeyVaultClient KeyVaultClient { get; }
        private IRuntimeConfigStorage RuntimeStorage { get; }

        public override async Task<string> Process(FlowDeploymentSession flowToDeploy)
        {
            var config = flowToDeploy.Config;
            var runtimeKeyVaultName = flowToDeploy.GetTokenString(PortConfigurationSettings.TokenName_RuntimeKeyVaultName);
            Ensure.NotNull(runtimeKeyVaultName, "runtimeKeyVaultName");

            var secretName = $"{config.Name}-inputschemafile";
            Configuration.TryGet(Constants.ConfigSettingName_SparkType, out string sparkType);
            var uriPrefix = (sparkType != null && sparkType == Constants.SparkTypeDataBricks) ? Constants.PrefixSecretScope : Constants.PrefixKeyVault;

            var schemaFileSecret = $"{uriPrefix}://{runtimeKeyVaultName}/{secretName}";
            flowToDeploy.SetStringToken(TokenName_InputSchemaFilePath, schemaFileSecret);

            await Task.CompletedTask;
            return "done";
        }
    }
}
