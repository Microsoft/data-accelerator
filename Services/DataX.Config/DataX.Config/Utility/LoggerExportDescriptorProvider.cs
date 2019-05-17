﻿// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Composition.Hosting.Core;
using System.Linq;
using System.Reflection;

namespace DataX.Config.Utility
{
    /// <summary>
    /// Using a LoggerFactory, dynamically creates Logger<T> to resolve for MEF
    /// Falls back to InstanceExportDescriptorProvider implementation if not a Logger
    /// </summary>
    public class LoggerAndInstanceExportDescriptorProvider : InstanceExportDescriptorProvider
    {
        private static readonly Type _ILoggerType = typeof(ILogger);

        private static readonly MethodInfo _CreateLogger =
            typeof(Microsoft.Extensions.Logging.LoggerFactoryExtensions)
            .GetMethods()
            .Where(m => m.Name == nameof(LoggerFactoryExtensions.CreateLogger) && m.IsGenericMethod)
            .FirstOrDefault();

        private readonly ILoggerFactory _loggerFactory;
        private readonly bool _hasInstances;

        public LoggerAndInstanceExportDescriptorProvider(object[] instances, ILoggerFactory loggerFactory)
            : base(instances)
        {
            _hasInstances = instances?.Length > 0;
            _loggerFactory = loggerFactory;
        }

        ///<inheritdoc />
        public override IEnumerable<ExportDescriptorPromise> GetExportDescriptors(CompositionContract contract, DependencyAccessor descriptorAccessor)
        {
            if (_loggerFactory != null && _ILoggerType.IsAssignableFrom(contract.ContractType))
            {
                ILogger logger;

                if (contract.ContractType.GenericTypeArguments.Length > 0)
                {
                    logger = CreateLogger(contract.ContractType.GenericTypeArguments.FirstOrDefault());
                }
                else
                {
                    logger = _loggerFactory.CreateLogger(contract.ContractType);
                }

                yield return new ExportDescriptorPromise(
                        contract,
                        contract.ContractType.FullName,
                        true,
                        NoDependencies,
                        dependencies => ExportDescriptor.Create((context, operation) => logger, NoMetadata));
            }
            else if(_hasInstances)
            {
                foreach (var descriptor in base.GetExportDescriptors(contract, descriptorAccessor))
                {
                    yield return descriptor;
                }
            }
        }

        private ILogger CreateLogger(Type t)
        {
            var genericMethod = _CreateLogger.MakeGenericMethod(t);
            return genericMethod.Invoke(null, new object[] { _loggerFactory }) as ILogger;
        }
    }
}
