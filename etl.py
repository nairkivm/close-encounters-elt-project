import luigi

import tasks as t

if __name__ == '__main__':
    luigi.build(
        [
            t.ExtractCloseEncountersDataTask(),
            t.SelectFieldsDataTask(),
            t.FormatFieldsTypeTask(),
            t.CleanDataTask(),
            t.TransformDataTask(),
            t.LoadDataTask()
        ],
        local_scheduler=False
    )
