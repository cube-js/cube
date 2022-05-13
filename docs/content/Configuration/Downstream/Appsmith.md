**Connecting to Appsmith**

 

[Appsmith](https://www.appsmith.com) is an open source framework for building internal tools. You can connect a Cube deployment to Appsmith using Cube’s [REST API](https://cube.dev/docs/rest-api).


## Use REST API in Cube

> Don't have a Cube project yet? [Learn how to get started here](https://cube.dev/docs/cloud/getting-started).


### Cube Cloud

Click the “How to connect” link on the Overview page, navigate to the REST API tab. You should see the screen below with your connection credentials (the REST API URL and the authorization token):



<p id="gdcalert1" ><span style="color: red; font-weight: bold">>>>>>  gd2md-html alert: inline image link here (to images/image1.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert2">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>>> </span></p>


![alt_text](images/image1.png "image_tooltip")


** **


### Self-hosted Cube

For a Cube instance publicly available at a specific `HOST`, the REST API URL would be `HOST/cubejs-api/v1`. Please refer to the [REST API page](https://cube.dev/docs/rest-api) for details.

You will also need to generate a JSON Web Token that would be used to authenticate requests to Cube. Please check the [Security page](https://cube.dev/docs/security#generating-json-web-tokens-jwt) to learn how to generate a token. For the best user experience of Appsmith users, it’s best to generate a long-lived JWT that would not expire anytime soon.


## Create a new Data Source in Appsmith

Copy and paste the REST API URL and the Authorization token to create a new data source in Appsmith. 

<p id="gdcalert2" ><span style="color: red; font-weight: bold">>>>>>  gd2md-html alert: inline image link here (to images/image2.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert3">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>>> </span></p>


![alt_text](images/image2.png "image_tooltip")



## Create a POST request in Appsmith 

Get your Cube query in the JSON [query format](https://cube.dev/docs/query-format) ready. You can copy it from Cube’s Playground or compose manually:



<p id="gdcalert3" ><span style="color: red; font-weight: bold">>>>>>  gd2md-html alert: inline image link here (to images/image3.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert4">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>>> </span></p>


![alt_text](images/image3.png "image_tooltip")


Create a POST request, paste the JSON Query in the Body and hit Run.



<p id="gdcalert4" ><span style="color: red; font-weight: bold">>>>>>  gd2md-html alert: inline image link here (to images/image4.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert5">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>>> </span></p>


![alt_text](images/image4.png "image_tooltip")



## Display the Data in Appsmith

You have many options to display the data in Appsmith. For instance, you can display the data in a table widget. Also, you can create a chart widget and map the values to _x_ and _y _coordinates accordingly, give a _title_ and _names _to the _axis_. 

<p id="gdcalert5" ><span style="color: red; font-weight: bold">>>>>>  gd2md-html alert: inline image link here (to images/image5.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert6">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>>> </span></p>


![alt_text](images/image5.png "image_tooltip")
