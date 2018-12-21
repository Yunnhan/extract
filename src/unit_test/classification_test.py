from classification.classification import Classification
from algorithm.create_df.read_data_lib.data_base import DataSQL
c = Classification()
d = DataSQL()

data = d.read_sql('select title, info, cate_id FROM stang_cbid WHERE id = 6304465')[0]
print(data[0])
print(c.main(data[0], data[1], data[2]))

