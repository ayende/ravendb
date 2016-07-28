using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public abstract class ConfigurationCategory
    {
        public const string DefaultValueSetInConstructor = "default-value-set-in-constructor";

        protected internal bool Initialized { get; set; }

        public virtual void Initialize(IConfigurationRoot settings)
        {
            var configurationProperties = from property in GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                let configurationEntryAttribute = property.GetCustomAttributes<ConfigurationEntryAttribute>().FirstOrDefault()
                where configurationEntryAttribute != null // filter out properties which aren't marked as configuration entry
                orderby configurationEntryAttribute.Order // properties are initialized in order of declaration
                select property;

            foreach (var property in configurationProperties)
            {
                TimeUnitAttribute timeUnit = null;
                SizeUnitAttribute sizeUnit = null;

                if (property.PropertyType == TimeSetting.TypeOf || property.PropertyType == TimeSetting.NullableTypeOf)
                {
                    timeUnit = property.GetCustomAttribute<TimeUnitAttribute>();
                    Debug.Assert(timeUnit != null);
                }
                else if (property.PropertyType == Size.TypeOf || property.PropertyType == Size.NullableTypeOf)
                {
                    sizeUnit = property.GetCustomAttribute<SizeUnitAttribute>();
                    Debug.Assert(sizeUnit != null);
                }

                var configuredValueSet = false;

                foreach (var entry in property.GetCustomAttributes<ConfigurationEntryAttribute>())
                {
                    var value = settings[entry.Key];

                    if (value == null)
                        continue;

                    try
                    {
                        var minValue = property.GetCustomAttribute<MinValueAttribute>();

                        if (minValue == null)
                        {
                            if (property.PropertyType.GetTypeInfo().IsEnum)
                            {
                                property.SetValue(this, Enum.Parse(property.PropertyType, value, true));
                            }
                            else if (timeUnit != null)
                            {
                                property.SetValue(this, new TimeSetting(Convert.ToInt64(value), timeUnit.Unit));
                            }
                            else if (sizeUnit != null)
                            {
                                property.SetValue(this, new Size(Convert.ToInt64(value), sizeUnit.Unit));
                            }
                            else
                            {
                                var t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                                var safeValue = (value == null) ? null : Convert.ChangeType(value, t);

                                property.SetValue(this, safeValue);
                            }
                        }
                        else
                        {
                            if (property.PropertyType == typeof(int) || property.PropertyType == typeof(int?))
                            {
                                property.SetValue(this, Math.Max(Convert.ToInt32(value), minValue.Int32Value));
                            }
                            else if (property.PropertyType == Size.TypeOf)
                            {
                                property.SetValue(this, new Size(Math.Max(Convert.ToInt32(value), minValue.Int32Value), sizeUnit.Unit));
                            }
                            else
                            {
                                throw new NotSupportedException("Min value for " + property.PropertyType + " is not supported. Property name: " + property.Name);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Could not set configuration value given under the following setting: " + entry.Key, e);
                    }

                    configuredValueSet = true;
                    break;
                }

                if (configuredValueSet)
                    continue;

                var defaultValueAttribute = property.GetCustomAttribute<DefaultValueAttribute>();

                if (defaultValueAttribute == null)
                {
                    throw new InvalidOperationException($"Property '{property.Name}' does not have a default value attribute");
                }

                var defaultValue = defaultValueAttribute.Value;

                if (DefaultValueSetInConstructor.Equals(defaultValue))
                    continue;

                if (timeUnit != null && defaultValue != null)
                {
                    property.SetValue(this, new TimeSetting(Convert.ToInt64(defaultValue), timeUnit.Unit));
                }
                else if (sizeUnit != null && defaultValue != null)
                {
                    property.SetValue(this, new Size(Convert.ToInt64(defaultValue), sizeUnit.Unit));
                }
                else
                {
                    property.SetValue(this, defaultValue);
                }
            }

            Initialized = true;
        }
    }
}