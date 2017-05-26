INSERT INTO hivetags
SELECT DISTINCT
  Time,
  Tag,
  Allcounts
From hiveint;
