<html>

<head>
	<meta http-equiv="Content-Type" content="text/html; charset=windows-1252">
</head>

<body>
	The words you entered are: <br>
	<% String s=request.getParameter("txtname"); if(s !=null) { String[] words=s.split(" ");
			int count = 0;
			for (String w : words) {
				count++;
				out.print(String.valueOf(count)+" : " + w + " <br>");
		}
		}
		%>


</body>

</html>