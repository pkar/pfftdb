window.onload = function() {
	var getHashValue = function(key) {
		return location.hash.match(new RegExp(key+'=([^&]*)'))[1];
	}
	var graphHash;
	try {
		graphHash = getHashValue("graph");
		if(graphHash === "") {
			graphHash = "user";
		}
	} catch(err) {
		console.log(err);
		graphHash = "user";
	}
	window.location.hash = "graph=" + graphHash;

	// Get a list of triples and setup the graph.
	var xhr = new XMLHttpRequest();
	xhr.onload = function (e) {
		if (xhr.readyState === 4) {
			if (xhr.status === 200) {
				var resp = JSON.parse(xhr.responseText);
				init(resp.data);
			} else {
				console.error(xhr.statusText);
			}
		}
	};
	xhr.onerror = function (e) {
		console.error(xhr.statusText);
	};
	xhr.open("POST", "/v1/triples");
	xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
	xhr.send(JSON.stringify({graph:graphHash, sub:"", pred:"", obj: null}));

	if ( ! Detector.webgl ) Detector.addGetWebGLMessage();
	var container, 
		stats, 
		camera, 
		controls, 
		scene, 
		renderer,
		layoutOptions,
		axes, 
		graph = new Graph({limit: 100000}), 
		geometry = new THREE.SphereGeometry(30, 30, 30),
		objectSelection,
		cross;

	var onWindowResize = function() {
		camera.aspect = window.innerWidth / window.innerHeight;
		camera.updateProjectionMatrix();
		renderer.setSize( window.innerWidth, window.innerHeight );
		controls.handleResize();
		render();
	}

  var render = function() {
		// generate layout if not finished.
    if(!graph.layout.finished) {
			graph.layout.generate();
		}

    for(var i=0; i<graph.nodes.length; i++) {
			var node = graph.nodes[i];
			if(node.data.label != undefined) {
				node.data.label.lookAt(camera.position);
			} 
		}

    // Update position of lines (edges)
    for(var i=0; i<graph.edges.length; i++) {
			var edge = graph.edges[i];
			/*
      edge.data.label.position.x = (edge.source.position.x + edge.target.position.x) / 2;
      edge.data.label.position.y = (edge.source.position.y + edge.target.position.y) / 2;
      edge.data.label.position.z = (edge.source.position.z + edge.target.position.z) / 2;
			*/
      edge.data.label.position.x = (edge.data.mesh.geometry.vertices[0].x + edge.data.mesh.geometry.vertices[1].x) / 2;
      edge.data.label.position.y = (edge.data.mesh.geometry.vertices[0].y + edge.data.mesh.geometry.vertices[1].y) / 2;
      edge.data.label.position.z = (edge.data.mesh.geometry.vertices[0].z + edge.data.mesh.geometry.vertices[1].z) / 2;

			edge.data.label.geometry.verticesNeedUpdate = true;
			edge.data.mesh.geometry.verticesNeedUpdate = true;
			edge.data.label.lookAt(camera.position);
    }

    //objectSelection.render(scene, camera);

		stats.update();
		renderer.render( scene, camera );
	}

	var animate = function() {
		requestAnimationFrame( animate );
		controls.update();
		render();
	}

	var buildAxis = function(src, dst, colorHex, dashed) {
		var geom = new THREE.Geometry(), mat; 

		if(dashed) {
		  mat = new THREE.LineDashedMaterial({ opacity: 0.5, linewidth: 3, color: colorHex, dashSize: 3, gapSize: 3 });
		} else {
			mat = new THREE.LineBasicMaterial({ opacity: 0.5, linewidth: 3, color: colorHex });
		}

		geom.vertices.push( src.clone() );
		geom.vertices.push( dst.clone() );
		geom.computeLineDistances(); // without this dashed lines will appear as simple plain lines

		var axis = new THREE.Line( geom, mat, THREE.LinePieces );
		return axis;
	}

	var buildAxes = function (length) {
		var axes = new THREE.Object3D();
		axes.add( buildAxis( new THREE.Vector3( 0, 0, 0 ), new THREE.Vector3( length, 0, 0 ), 0xFF0000, false ) ); // +X
		axes.add( buildAxis( new THREE.Vector3( 0, 0, 0 ), new THREE.Vector3( -length, 0, 0 ), 0xFF0000, true) ); // -X
		axes.add( buildAxis( new THREE.Vector3( 0, 0, 0 ), new THREE.Vector3( 0, length, 0 ), 0x00FF00, false ) ); // +Y
		axes.add( buildAxis( new THREE.Vector3( 0, 0, 0 ), new THREE.Vector3( 0, -length, 0 ), 0x00FF00, true ) ); // -Y
		axes.add( buildAxis( new THREE.Vector3( 0, 0, 0 ), new THREE.Vector3( 0, 0, length ), 0x0000FF, false ) ); // +Z
		axes.add( buildAxis( new THREE.Vector3( 0, 0, 0 ), new THREE.Vector3( 0, 0, -length ), 0x0000FF, true ) ); // -Z
		return axes;
	}

	var init = function(triples) {
		// renderer
		renderer = new THREE.WebGLRenderer( { antialias: true } );
		//renderer.setClearColor( scene.fog.color, 1 );
		renderer.setClearColorHex( 0x000000, 1.0 );
		renderer.setSize( window.innerWidth, window.innerHeight );

		// add renderer to container
		container = document.getElementById( 'container' );
		container.appendChild( renderer.domElement );

		// camera
		camera = new THREE.PerspectiveCamera( 60, window.innerWidth / window.innerHeight, 1, 10000 );
		camera.position.z = 800;

		// controls
		controls = new THREE.TrackballControls( camera );
		controls.rotateSpeed = 1.0;
		controls.zoomSpeed = 1.2;
		controls.panSpeed = 0.8;
		controls.noZoom = false;
		controls.noPan = false;
		controls.staticMoving = true;
		controls.dynamicDampingFactor = 0.3;
		controls.keys = [ 65, 83, 68 ];
		controls.addEventListener( 'change', render );

		// world
		scene = new THREE.Scene();
		//scene.fog = new THREE.FogExp2( 0xcccccc, 0.002 );
		
		// Add x y z axes
		axes = buildAxes(2000);
		scene.add(axes);
		//scene.remove(axes);

		// object click handler
		objectSelection = new THREE.ObjectSelection({
			domElement: renderer.domElement,
			selected: function(obj) {
				console.log(obj);
			},
			clicked: function(obj) {
				console.log(obj);
			}
		});

		// lights
		light = new THREE.DirectionalLight( 0xffffff );
		light.position.set( 1, 1, 1 );
		scene.add( light );
		light = new THREE.DirectionalLight( 0x002288 );
		light.position.set( -1, -1, -1 );
		scene.add( light );
		light = new THREE.AmbientLight( 0x222222 );
		scene.add( light );

		// stats
		stats = new Stats();
		stats.domElement.style.position = 'absolute';
		stats.domElement.style.top = '0px';
		stats.domElement.style.zIndex = 100;
		container.appendChild( stats.domElement );

		window.addEventListener( 'resize', onWindowResize, false );

		// Initialize the graph and draw nodes and edges.
		initGraph(graph, triples);
		
		// start animation
		animate();
	}

  var initGraph = function(graph, triples) {
		var steps = 0;
		var nodes = {};
		for(var i=0; i < triples.length; i++) {
			if(!nodes[triples[i][0]]) {
				steps++;
				nodes[triples[i][0]] = newNode(triples[i][0], steps);
			}
			if(!nodes[triples[i][2]]) {
				steps++;
				nodes[triples[i][2]] = newNode(triples[i][2], steps);
			}

			var nodeSub = nodes[triples[i][0]];
			var nodeObj = nodes[triples[i][2]];

			var edge = graph.addEdge(nodeSub, nodeObj)
			if(edge) {
				edge.data.title = triples[i][1];
				drawEdge(edge);
			}
		}
    
		layoutOptions = {width: 1000, height: 1000, iterations: 2000, layout: "3d"};
    graph.layout = new Layout.ForceDirected(graph, layoutOptions);
    graph.layout.init();
  }

	var newNode = function(title, id) {
		var node = new Node(id);
		node.data.title = title;
		graph.addNode(node);
		drawNode(node);
		return node;
	}

  var drawNode = function(node) {
		var material = new THREE.MeshLambertMaterial( { color: Math.random() * 0xffffff, opacity: 0.6 } );
		var mesh = new THREE.Mesh( geometry, material );
		mesh.position.x = ( Math.random() - 0.5 ) * 500;
		mesh.position.y = ( Math.random() - 0.5 ) * 500;
		mesh.position.z = ( Math.random() - 0.5 ) * 500;
		mesh.updateMatrix();
		mesh.matrixAutoUpdate = true;

    mesh.id = node.id;
    node.data.mesh = mesh;
    node.position = mesh.position;
		scene.add( node.data.mesh );

    var label = new THREE.Label(node.data.title);
		label.position = node.position;
    node.data.label = label;
    scene.add( node.data.label );
  }

  var drawEdge = function(edge) {
		var material = new THREE.LineBasicMaterial( { color: 0xa020f0, opacity: 0.9, linewidth: 0.9 } );

		var tmpGeo = new THREE.Geometry();
		tmpGeo.vertices.push(edge.source.position);
		tmpGeo.vertices.push(edge.target.position);

		var mesh = new THREE.Line( tmpGeo, material );
		mesh.scale.x = mesh.scale.y = mesh.scale.z = 1;
		mesh.originalScale = 1;

		//mesh.dynamic = true;
		
		edge.data.mesh = mesh;
		scene.add( mesh );

    var label = new THREE.Label(edge.data.title, {fillStyle: "#888888"});
		label.position = edge.source.position.clone();
    edge.data.label = label;
    scene.add( edge.data.label );
  }
}
