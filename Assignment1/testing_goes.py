def file_URL(filename):
    x=filename.split("_")
    goes=x[2][1:]
    prod=x[1].split("-")
    if prod[2][-1].isdigit():
        prod[2]=prod[2][:-1]
    year=x[3][1:5]
    day=x[3][5:8]
    hour=x[3][8:10]
    return "https://noaa-goes"+goes+".s3.amazonaws.com/"+prod[0]+"-"+prod[1]+"-"+prod[2]+"/"+year+"/"+day+"/"+hour+"/"+filename

def test_01():
    assert file_URL("OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMM/2023/009/05/OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc"

def test_02():
    assert file_URL("OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACTPM/2023/009/04/OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc"

def test_03():
    assert file_URL("OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSIM/2023/011/06/OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc"

def test_04():
    assert file_URL("OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTM/2022/356/08/OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc"

def test_05():
    assert file_URL("OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-BRFF/2022/315/02/OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc"

def test_06():
    assert file_URL("OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-ADPM/2023/006/13/OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc"

def test_07():
    assert file_URL("OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadM/2023/003/02/OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc"

def test_08():
    assert file_URL("OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTF/2022/353/22/OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc"

def test_09():
    assert file_URL("OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSRC/2022/318/05/OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc"

def test_10():
    assert file_URL("OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-DMWVM/2022/355/20/OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc"

def test_11():
    assert file_URL("OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMC/2022/280/09/OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc"

def test_12():
    assert file_URL("OR_ABI-L2-DMWC-M6C07_G18_s20223510516174_e20223510518559_c20223510527449.nc")=="https://noaa-goes18.s3.amazonaws.com/ABI-L2-DMWC/2022/351/05/OR_ABI-L2-DMWC-M6C07_G18_s20223510516174_e20223510518559_c20223510527449.nc"