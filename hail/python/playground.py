import hail as hl
from hail.plot import show

import pyspark


#scon = pyspark.context.SparkContext("local[*]")

# hl.init()
#
# covariates = hl.import_table('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogistic.cov',
#                                      key='Sample',
#                                      types={'Cov1': hl.tfloat, 'Cov2': hl.tfloat})
#
# pheno = hl.import_table('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogisticBoolean.pheno',
#                                 key='Sample',
#                                 missing='0',
#                                 types={'isCase': hl.tbool})
#
# mt = hl.import_vcf('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogistic.vcf')
#
# ht = hl.logistic_regression_rows('wald',
#                                          y=[pheno[mt.s].isCase],
#                                          x=mt.GT.n_alt_alleles(),
#                                          covariates=[1.0, covariates[mt.s].Cov1, covariates[mt.s].Cov2])
#
# results = dict(hl.tuple([ht.locus.position, ht.row]).collect())

# -- TileDB Regression START ---
# covariates = hl.import_table('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogistic.cov',
#                                      key='Sample',
#                                      types={'Cov1': hl.tfloat, 'Cov2': hl.tfloat})

# pheno = hl.import_table('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogisticBoolean.pheno',
#                                 key='Sample',
#                                 missing='0',
#                                 types={'isCase': hl.tbool})
#
# ht = hl.logistic_regression_rows('wald',
#                                          y=[pheno[mtiledb.s].isCase],
#                                          x=mtiledb.fmt_GT.n_alt_alleles(),
#                                          covariates=[1.0, covariates[mtiledb.s].Cov1, covariates[mtiledb.s].Cov2])
#
# results = dict(hl.tuple([ht.locus.position, ht.row]).collect())

# -- TileDB Regression END ---




path1 = '/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/1kg/1kg_tiledb'
samples1 = "NA20318"

path2 = '/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/arrays/v3/ingested_2samples'
samples2 = "HG01762"

path3 = '/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/hail_vcf/regression_logistic_tiledb'
samples3 = "A"



#mt1 = hl.import_vcf('/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/small.vcf')
#mt1 = hl.import_vcf('/Users/victor/Dev/tiledb/TileDB-VCF/libtiledbvcf/test/inputs/1kg/original/1kg.vcf')
mt1 = hl.import_vcf('/Users/victor/Dev/tiledb/hail/hail/src/test/resources/regressionLogistic.vcf')

# print(mt1.GT.collect())
# print(mt1.DP.collect())
# print(mt1.PL.collect())

print("-------------------------*:::**::**:::*-------------------------")

mt2 = hl.import_vcf(path=path2, samples=samples2, tiledb=True)

def test_info_fields():
    mt = hl.sample_qc(mt2)

    gt = mt.GT.take(10)
    dp = mt.DP.take(10)
    pl = mt.PL.take(10)

    print(gt)
    print(dp)
    print(pl)

    mt.col.describe()

    p = hl.plot.histogram(mt.sample_qc.call_rate, range=(.88,1), legend='Call Rate')

    show(p)

test_info_fields()