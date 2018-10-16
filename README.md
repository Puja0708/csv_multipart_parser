<b> CSVMultiPartParser </b> is a custom drf parser (like Multipart Parser) which allows you to upload a csv file (along with raw json data if you want) .

# usage
- inside the view class, just specify the parser class as `CSVMultiPartParser`
	```
		parser_classes = (CSVMultiPartParser,)
	```

- this will gice you the parsed values of the csv keys in `request.data['file']`
- if you want the dict represetation of the same (as in, csv values with headers), do `request.csv_with_keys = True` before accessing the request.data

## TODO : make it a package