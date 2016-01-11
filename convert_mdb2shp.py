#!/usr/bin/env python

import ogr, gdal, osr

# Open SHP

source_ds = ogr.Open("/Users/rory/Documents/ogr2ogr_test/ocha_shp_21050524/ocha_aoi_20150524.shp")
source_layer = source_ds.GetLayer()
print source_layer

# David please add some if/else here to create a new column

# Create the Raster

pixelWidth = pixelHeight = 0.01 # depending how fine you want your raster
x_min, x_max, y_min, y_max = source_layer.GetExtent()
cols = int((x_max - x_min) / pixelHeight)
rows = int((y_max - y_min) / pixelWidth)
target_ds = gdal.GetDriverByName('GTiff').Create('/Users/rory/Documents/ogr2ogr_test/yourRaster2.tif', cols, rows, 1, gdal.GDT_Byte)
target_ds.SetGeoTransform((x_min, pixelWidth, 0, y_min, 0, pixelHeight))
band = target_ds.GetRasterBand(1)
NoData_value = 999999
band.SetNoDataValue(NoData_value)
band.FlushCache()


# Rasterize the SHP

gdal.RasterizeLayer(target_ds, [1], source_layer, options = ["ATTRIBUTE=Val"])

# Add a spatial reference (doesn't work yet)

target_dsSRS = osr.SpatialReference()
target_dsSRS.ImportFromEPSG(4326)
target_ds.SetProjection(target_dsSRS.ExportToWkt())
