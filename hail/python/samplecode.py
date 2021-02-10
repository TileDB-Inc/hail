import hail as hl

hl.init()

covariates = hl.import_table('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogistic.cov',
                                     key='Sample',
                                     types={'Cov1': hl.tfloat, 'Cov2': hl.tfloat})

pheno = hl.import_table('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogisticBoolean.pheno',
                                key='Sample',
                                missing='0',
                                types={'isCase': hl.tbool})

mt = hl.import_vcf('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogistic.vcf')
