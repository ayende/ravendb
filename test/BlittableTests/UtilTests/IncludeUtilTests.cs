using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace BlittableTests.UtilTests
{
    public class IncludeUtilTests
    {
        [Fact]
        public async Task FindDocIdFromPath_should_not_work_with_two_adjacent_delimiters()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1",
                    ["AddressInfoId"] = "addresses/1"
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {				
                Assert.Throws<InvalidOperationException>(
                    () => IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo..ContactInfoId").ToList());
                Assert.Throws<InvalidOperationException>(
                    () => IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo,,ContactInfoId").ToList());
                Assert.Throws<InvalidOperationException>(
                    () => IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.,ContactInfoId").ToList());
                Assert.Throws<InvalidOperationException>(
                    () => IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.,ContactInfoId").ToList());
                Assert.Throws<InvalidOperationException>(
                    () => IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.,.ContactInfoId").ToList());
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_return_empty_for_incorrect_path()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1"
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                Assert.Empty(IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId"));
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1"
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId").First();
                Assert.Equal("contacts/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_with_multiple_targets_should_work_in_flat_object()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ContactInfoId"] = "contacts/1",
                ["AddressInfoId"] = "addresses/1",
                ["CarInfoId"] = "cars/1"
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "AddressInfoId").First();
                Assert.Equal("addresses/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ContactInfoId").First();
                Assert.Equal("contacts/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "CarInfoId").First();
                Assert.Equal("cars/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_one_level_sub_property()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1",
                    ["AddressInfoId"] = "addresses/1"
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId").First();
                Assert.Equal("contacts/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.AddressInfoId").First();
                Assert.Equal("addresses/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_multiple_one_level_sub_properties1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1",
                    ["AddressInfoId"] = "addresses/1"
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.ContactInfoId,ExtendedInfo.AddressInfoId").ToList();
                Assert.Contains(ids, id => id == "contacts/1");
                Assert.Contains(ids, id => id == "addresses/1");
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_multiple_one_level_sub_properties2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1",
                    ["AddressInfoId"] = "addresses/1",
                    ["Foo"] = "Bar"
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo.Foo,ExtendedInfo.AddressInfoId, ExtendedInfo.ContactInfoId").ToList();
                Assert.Contains(ids, id => id == "Bar");
                Assert.Contains(ids, id => id == "contacts/1");
                Assert.Contains(ids, id => id == "addresses/1");
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_multiple_deep_sub_properties()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["ExtendedInfo"] = new DynamicJsonValue
                {
                    ["ContactInfoId"] = "contacts/1"
                },
                ["ExtendedInfo2"] = new DynamicJsonValue
                {
                    ["SubProp"] = new DynamicJsonValue {
                        ["AddressInfoId"] = "addresses/1"
                    }
                },
                ["ExtendedInfo3"] = new DynamicJsonValue
                {
                    ["SubProp"] = new DynamicJsonValue
                    {
                        ["SubProp2"] = new DynamicJsonValue
                        {
                            ["Foo"] = "Bar"
                        }
                    }
                },

            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var ids = IncludeUtil.GetDocIdFromInclude(reader, "ExtendedInfo3.SubProp.SubProp2.Foo,ExtendedInfo2.SubProp.AddressInfoId, ExtendedInfo.ContactInfoId").ToList();
                Assert.Contains(ids, id => id == "Bar");
                Assert.Contains(ids, id => id == "contacts/1");
                Assert.Contains(ids, id => id == "addresses/1");
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_multi_level_sub_property1()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["FooBarSubObject1"] = new DynamicJsonValue
                {
                    ["ExtendedInfo"] = new DynamicJsonValue
                    {
                        ["AddressInfoId"] = "addresses/1"
                    }
                },
                ["FooBarSubObject2"] = new DynamicJsonValue
                {
                    ["ExtendedInfo"] = new DynamicJsonValue
                    {
                        ["ContactInfoId"] = "contacts/1",
                    }
                }
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "FooBarSubObject1.ExtendedInfo.AddressInfoId").First();
                Assert.Equal("addresses/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "FooBarSubObject2.ExtendedInfo.ContactInfoId").First();
                Assert.Equal("contacts/1", id);
            }
        }

        [Fact]
        public async Task FindDocIdFromPath_should_work_in_multi_level_sub_property2()
        {
            var obj = new DynamicJsonValue
            {
                ["Name"] = "John Doe",
                ["FooBarSubObject1"] = new DynamicJsonValue
                {
                    ["ExtendedInfo"] = new DynamicJsonValue
                    {
                        ["AddressInfoId"] = "addresses/1"
                    },
                    ["FooBarSubObject2"] = new DynamicJsonValue
                    {
                        ["ExtendedInfo"] = new DynamicJsonValue
                        {
                            ["ContactInfoId"] = "contacts/1",
                        }
                    }
                },
                
            };
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new RavenOperationContext(pool))
            using (var reader = await context.ReadObject(obj, "foo"))
            {
                var id = IncludeUtil.GetDocIdFromInclude(reader, "FooBarSubObject1.ExtendedInfo.AddressInfoId").First();
                Assert.Equal("addresses/1", id);
                id = IncludeUtil.GetDocIdFromInclude(reader, "FooBarSubObject1.FooBarSubObject2.ExtendedInfo.ContactInfoId").First();
                Assert.Equal("contacts/1", id);
            }
        }
    }
}
