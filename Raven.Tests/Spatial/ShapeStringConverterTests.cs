﻿using Raven.Abstractions.Indexing;
using Raven.Database.Indexing.Spatial;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class ShapeStringConverterTests
	{
		[Fact]
		public void ParseBox()
		{
			var converter = new ShapeStringConverter(new SpatialOptions());
			var result1 = converter.ConvertToWKT("BOX(35.6 0.0, 45.9 79.4)");
			Assert.Equal("35.6 0.0 45.9 79.4", result1);
			var result2 = converter.ConvertToWKT("BOX2D(35.6 0.0, 45.9 79.4)");
			Assert.Equal("35.6 0.0 45.9 79.4", result2);
		}

		[Fact]
		public void ParseGeoUriKilometers()
		{
			var converter = new ShapeStringConverter(new SpatialOptions() { Units = SpatialUnits.Kilometers });
			var result1 = converter.ConvertToWKT("geo:-45.8,12.5,65.0");
			Assert.Equal("POINT (12.5 -45.8)", result1);
			var result2 = converter.ConvertToWKT("geo:-45.8,12.5,65.0;u=1000");
			Assert.Equal("Circle(12.5 -45.8 d=1)", result2);
		}

		[Fact]
		public void ParseGeoUriMiles()
		{
			var converter = new ShapeStringConverter(new SpatialOptions() { Units = SpatialUnits.Miles });
			var result1 = converter.ConvertToWKT("geo:-45.8,12.5,65.0");
			Assert.Equal("POINT (12.5 -45.8)", result1);
			var result2 = converter.ConvertToWKT("geo:-45.8,12.5,65.0;u=1000");
			Assert.Equal("Circle(12.5 -45.8 d=0.621371)", result2);
		}
	}
}
