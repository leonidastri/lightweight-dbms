Select DISTINCT S.B, B.E, R.G from Sailors S, Boats B, Reserves R WHERE S.B < B.D AND B.E = R.G ORDER BY S.B, B.E;