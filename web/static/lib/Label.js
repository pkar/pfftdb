/**
 * NOTE: Modified from original.
 *
  @author David Piegza

  Implements a label for an object.
  
  It creates an text in canvas and sets the text-canvas as
  texture of a cube geometry.
  
  Parameters:
  text: <string>, text of the label
  
  Example: 
  var label = new THREE.Label("Text of the label");
  label.position.x = 100;
  label.position.y = 100;
  scene.addObject(label);
 */

THREE.Label = function(text, parameters) {
  var parameters = parameters || {};
  
  var labelCanvas = document.createElement( "canvas" );
  
  function create() {
    var xc = labelCanvas.getContext("2d");
    xc.font = "12pt Arial";
    xc.fillStyle = parameters.fillStyle || "#FFFFFF";
    xc.textBaseline = 'top';
    xc.fillText(text, 0, 0);

		var texture = new THREE.Texture(labelCanvas)
		texture.needsUpdate = true;
		var material = new THREE.MeshBasicMaterial( {map: texture, side:THREE.DoubleSide } );
    material.transparent = true;
		var mesh = new THREE.Mesh(
      new THREE.PlaneGeometry(labelCanvas.width, labelCanvas.height),
			material
    );
		return mesh;
  }

  return create();
}
