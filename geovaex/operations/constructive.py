class Constructive:
    """Constructive Geometric Operations.

    Applies PyGEOS constructive geometric operations to GeoDataFrame.

    Attributes:
        df (object): A GeoDataFrame object.
    """
    def __init__(self, df):
        self.df = df

    def _geometry_constructive(self, operation, *args, inplace=False, **kwargs):
        """Performs various constructive operations over geometries.
        Parameters:
            operation (string): The name of the constructive operation.
            *args: Extra arguments required by the operation.
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
            **kwargs: Extra keyword arguments for the constructive operation.
        Returns:
            (object) GeoDataFrame
        """
        df = self.df if inplace else self.df.trim()
        df._geoseries = df.geometry._constructive(operation, *args, **kwargs)
        return df

    def boundary(self, inplace=False, **kwargs):
        """Returns the topological boundary of a geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('boundary', inplace=inplace, **kwargs)

    def buffer(self, radius, quadsegs=8, cap_style='round', join_style='round', mitre_limit=5.0, single_sided=False, inplace=False, **kwargs):
        """Computes the buffer of a geometry for positive and negative buffer radius.
        Parameters:
            radius (float|array): Specifies the circle radius in the Minkowski sum (or difference).
            quadsegs (int): Specifies the number of linear segments in a quarter circle in the approximation of circular arcs.
            cap_style (string): One of ‘round’, ‘square’, ‘flat’.
                Specifies the shape of buffered line endings. ‘round’ results in circular line endings (see quadsegs).
                Both ‘square’ and ‘flat’ result in rectangular line endings, only ‘flat’ will end at the original vertex,
                while ‘square’ involves adding the buffer width.
            join_style (string): One of ‘round’, ‘bevel’, ‘sharp’. Specifies the shape of buffered line midpoints.
                ‘round’ results in rounded shapes.
                ‘bevel’ results in a beveled edge that touches the original vertex.
                ‘mitre’ results in a single vertex that is beveled depending on the mitre_limit parameter.
            mitre_limit ('float'): Crops of ‘mitre’-style joins if the point is displaced from the buffered vertex by more than this limit.
            single_sided (bool): Only buffer at one side of the geometry.
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('buffer', radius, quadsegs=quadsegs, cap_style=cap_style, join_style=join_style, mitre_limit=mitre_limit, single_sided=single_sided, inplace=inplace, **kwargs)

    def build_area(self, inplace=False, **kwargs):
        """Creates an areal geometry formed by the constituent linework of given geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('build_area', inplace=inplace, **kwargs)

    def centroid(self, inplace=False, **kwargs):
        """Computes the geometric center (center-of-mass) of a geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('centroid', inplace=inplace, **kwargs)

    def clip_by_rect(self, xmin, ymin, xmax, ymax, inplace=False, **kwargs):
        """Returns the portion of a geometry within a rectangle.
        Parameters:
            xmin (float): Minimum x value of the rectangle.
            ymin (float): Minimum y value of the rectangle.
            xmax (float): Maximum x value of the rectangle.
            ymax (float): Maximum y value of the rectangle.
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('clip_by_rect', xmin, ymin, xmax, ymax, inplace=inplace, **kwargs)

    def convex_hull(self, inplace=False, **kwargs):
        """Computes the minimum convex geometry that encloses an input geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('convex_hull', inplace=inplace, **kwargs)

    def delaunay_triangles(self, inplace=False, tolerance=0.0, only_edges=False, **kwargs):
        """Computes a Delaunay triangulation around the vertices of an input geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
            tolerance (float|array): Snap input vertices together if their distance is less than this value.
            only_edges (bool|array): If set to True, the triangulation will return a collection of linestrings instead of polygons.
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('delaunay_triangles', inplace=inplace, tolerance=tolerance, only_edges=only_edges, **kwargs)

    def envelope(self, inplace=False, **kwargs):
        """Computes the minimum bounding box that encloses an input geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('envelope', inplace=inplace, **kwargs)

    def extract_unique_points(self, inplace=False, **kwargs):
        """Returns all distinct vertices of an input geometry as a multipoint.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('extract_unique_points', inplace=inplace, **kwargs)

    def make_valid(self, inplace=False, **kwargs):
        """Repairs invalid geometries.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('make_valid', inplace=inplace, **kwargs)

    def normalize(self, inplace=False, **kwargs):
        """Converts Geometry to normal or canonical form.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('normalize', inplace=inplace, **kwargs)

    def offset_curve(self, distance, quadsegs=8, join_style='round', mitre_limit=5.0, inplace=False, **kwargs):
        """Returns a (Multi)LineString at a distance from the object on its right or its left side.
        Parameters:
            distance (float|array): Specifies the offset distance from the input geometry.
                Negative for right side offset, positive for left side offset.
            quadsegs (int): Specifies the number of linear segments in a quarter circle in the approximation of circular arcs.
            join_style (string): One of ‘round’, ‘bevel’, ‘sharp’. Specifies the shape of outside corners.
                ‘round’ results in rounded shapes.
                ‘bevel’ results in a beveled edge that touches the original vertex.
                ‘mitre’ results in a single vertex that is beveled depending on the mitre_limit parameter.
            mitre_limit (float): Crops of ‘mitre’-style joins if the point is displaced from the buffered vertex by more than this limit.
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('offset_curve', distance, quadsegs=quadsegs, join_style=join_style, mitre_limit=mitre_limit, inplace=inplace, **kwargs)

    def point_on_surface(self, inplace=False, **kwargs):
        """Returns a point that intersects an input geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('point_on_surface', inplace=inplace, **kwargs)

    def reverse(self, inplace=False, **kwargs):
        """Returns a copy of a Geometry with the order of coordinates reversed.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('reverse', inplace=inplace, **kwargs)

    def simplify(self, tolerance, preserve_topology=False, inplace=False, **kwargs):
        """Returns a simplified version of an input geometry using the Douglas-Peucker algorithm.
        Parameters:
            tolerance (float|array): The maximum allowed geometry displacement.
                The higher this value, the smaller the number of vertices in the resulting geometry.
            preserve_topology (bool): If set to True, the operation will avoid creating invalid geometries.
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('simplify', tolerance, preserve_topology=preserve_topology, inplace=inplace, **kwargs)

    def snap(self, reference, tolerance, inplace=False, **kwargs):
        """Snaps an input geometry to reference geometry’s vertices.
        Parameters:
            reference (object): PyGeos geometry or array-like.
            tolerance (float|array): The tolerance is used to control where snapping is performed.
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('snap', reference, tolerance, inplace=inplace, **kwargs)

    def voronoi_polygons(self, inplace=False, tolerance=0.0, extend_to=None, only_edges=False, **kwargs):
        """Computes a Voronoi diagram from the vertices of an input geometry.
        Parameters:
            inplace (bool): If True, the geometry column would be changed by the result.
                Otherwise, a new GeoDataFrame is returned (default: False).
            tolerance (float|array): Snap input vertices together if their distance is less than this value.
            extend_to (object): PyGeos geometry or array-like. If provided, the diagram will be extended to
                cover the envelope of this geometry (unless this envelope is smaller than the input geometry).
            only_edges (bool): If set to True, the triangulation will return a collection of linestrings instead of polygons.
        Returns:
            (object) GeoDataFrame
        """
        return self._geometry_constructive('voronoi_polygons', inplace=inplace, tolerance=tolerance,
                                           extend_to=extend_to, only_edges=only_edges, **kwargs)
