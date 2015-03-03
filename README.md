# Spark Scifio

A Spark-interface for the io.scif and other libraries to take advantage of the ImageJ2 ecosystem.

```{scala}
import fourquant.io.IOOps._
val fImages = sc.floatImages("cellImgs/*.tif")
```

It also has a more generic-style support
```{scala}
val pCellImages = sc.genericImages[Double,DoubleType]("cellimages/*.tif", () => new DoubleType)
```



