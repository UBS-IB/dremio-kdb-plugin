
trade:([]time:`time$();sym:`symbol$();price:`float$();size:`int$())

`trade insert(09:30:00.000;`a;10.75;100)
`trade insert(09:31:00.000;`a;10.85;120)
`trade insert(09:32:00.000;`b;12.75;200)
select sum size by sym from trade
