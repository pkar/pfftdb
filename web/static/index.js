var redraw;

$(function() {
	var getHashValue = function(key) {
		return location.hash.match(new RegExp(key+'=([^&]*)'))[1];
	}

	var canvas = document.getElementById('canvas');
	var g = new Graph();
	var layouter = new Graph.Layout.Spring(g);
	var renderer = new Graph.Renderer.Raphael('canvas', g, canvas.scrollWidth, canvas.scrollHeight);

	$("#refresh").click(function(){
		redraw();
	});

	redraw = function() {
		layouter.layout();
		renderer.draw();
	}


	/*
	*/

	var graph;
	try {
		graph = getHashValue("graph");
		if(graph === "") {
			graph = "user";
		}
	} catch(err) {
		console.log(err);
		graph = "user";
	}
	window.location.hash = "graph=" + graph;

	$.ajax({
		type: "POST",
		url: "/v1/triples",
		dataType: 'json',
		data: JSON.stringify({graph:graph, sub:"", pred:"", obj: null}),
		success: function(resp) {
			if(resp.data !== null) {
				for (var i=0; i < resp.data.length; i++) {
					var t = resp.data[i];
					g.addEdge(t[0], t[2], {directed: true, label: t[1]});
				}
				redraw();
			} else {
				$.ajax({
					type: "GET",
					url: "/v1/graphs",
					dataType: 'json',
					success: function(resp) {
						console.log("current graphs\n" + resp.data);
					}
				});
				$("#canvas").prepend("no results");
			}
		}
	});
});
