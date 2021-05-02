<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" trimDirectiveWhitespaces="true" session="true" %>
<%@ page import="mypackage.*"%>
<%@ page import="java.util.*" %> 

<html>

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, shrink-to-fit=no">
    <title>Search Result</title>
    <link rel="stylesheet" href="./assets/bootstrap/css/bootstrap.min.css">
    <link rel="stylesheet"
        href="https://fonts.googleapis.com/css?family=Open+Sans:300italic,400italic,600italic,700italic,800italic,400,300,600,700,800">
    <link rel="stylesheet"
        href="https://fonts.googleapis.com/css?family=Merriweather:400,300,300italic,400italic,700,700italic,900,900italic">
    <link rel="stylesheet"
        href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <link rel="stylesheet"
        href="https://cdnjs.cloudflare.com/ajax/libs/magnific-popup.js/1.1.0/magnific-popup.min.css">
    <link rel="stylesheet" href="./assets/css/home.css">
</head>

<body id="page-top"
    style="background-image: url('./assets/img/bg0.jpg');background-attachment: fixed;background-size: cover;">
    <!------------ Navigation Bar ------------>
    <nav class="navbar navbar-light navbar-expand-lg fixed-top" id="mainNav">
        <div class="container"><a class="navbar-brand js-scroll-trigger" href='./index.html'>.txtr</a>
            <div class="col">
                <form method="post" action="./search.jsp">
                    <div class="input-group">
                        <div class="input-group-prepend"></div><input class="form-control searchInputBox"
                            type="text" placeholder="Enter search"
                            value='<%=request.getParameter("userInput")%>' name="userInput" autofocus />
                        <div class="input-group-append"><button class="btn btn-primary searchBtn"
                                type="submit" style="border-radius: 0;"><i
                                    class="fa fa-search"></i></button></div>
                </form>
            </div>
        </div>
        <div class="collapse navbar-collapse" id="navbarResponsive">
            <ul class="navbar-nav ml-auto">
                <li class="nav-item"></li>
                <li class="nav-item"></li>
                <li class="nav-item"></li>
                <li class="nav-item"></li>
            </ul>
        </div>
        </div>
    </nav>
    <!------------ Display Result ------------>
    <div>
      <%
        StopStem stopstem = new StopStem("stopwords-en.txt"); // Put this file under bin/ folder since it is the working folder at the moment.
        String query = "";
        if(request.getParameter("userInput")!=null) {
        	query = request.getParameter("userInput");
            // out.println("<br><br><br><div> Your input: "+ query + "</div>");
            String[] splitted = query.split(" ");
            for (int i = 0; i < splitted.length; i++){
                splitted[i] = stopstem.stem(splitted[i]);
            }
            query = String.join(" ",splitted);
			// out.println("<div> Stem: " + query  + "</div>");
		} else {
			out.println("You input nothing.");
		}       

        InvertedIndex inv = new InvertedIndex();
        inv.openAllDB();
        
        // out.println("<div>" + inv.sayHaHa(query) + "</div>");
        Map<String, Double> result = inv.rankingAlgorithm(query);
        //for(Map.Entry<String, Double> entry : result.entrySet()){
        //    String url = inv.getURLbyDocID("doc" + entry.getKey());
        //    out.println(inv.getTitlebyURL(url));
        //}
        //    out.println("<div>" + result.isEmpty() + "</div>");
      %>
      
    
    </div>
    
        <header class=" text-white d-flex"
            style="/*background-image: url('./assets/img/header.jpg');*/height: 100vh;">
            <div class="container p-0">
                <div class="col-lg mx-auto resultList">
                    <% for(Map.Entry<String, Double> entry : result.entrySet()){ %>
                        <div class="row">
                            <div class="col">
                                <div class="card resultCard">
                                    <div class="card-body">
                                        <h4 class="card-title">
                                            <% String url = inv.getURLbyDocID("doc" + entry.getKey()); %>
                                            Title: <% out.println(inv.getTitlebyURL(url)); %>
                                        </h4>
                                        <h6 class="text-muted card-subtitle mb-2">Subtitle</h6>
                                        <p class="card-text">Score: <% out.println(entry.getValue()); %></p>
                                        <a class="card-link" href="<%=url%>" >Go to site</a>
                                    </div>
                                </div>
                            </div>
                        </div>
                    <% } %>
                </div>
            </div>
        </header>

        <!-- Close Database -->
        <% inv.closeAllDB();%>

        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
        <script
            src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.6.0/js/bootstrap.bundle.min.js"></script>
        <script
            src="https://cdnjs.cloudflare.com/ajax/libs/jquery-easing/1.4.1/jquery.easing.min.js"></script>
        <script
            src="https://cdnjs.cloudflare.com/ajax/libs/magnific-popup.js/1.1.0/jquery.magnific-popup.min.js"></script>
        <script src="./assets/js/creative.js"></script>
</body>

</html>
