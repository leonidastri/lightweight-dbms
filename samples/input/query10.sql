Select DISTINCT Sailors.B, Boats.E, Reserves.G from Sailors, Boats, Reserves WHERE Sailors.B < Boats.D AND Boats.E = Reserves.G ORDER BY Sailors.B, Boats.E;