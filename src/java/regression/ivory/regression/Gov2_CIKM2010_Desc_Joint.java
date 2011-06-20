package ivory.regression;

import ivory.eval.Qrels;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.HashMap;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.umd.cloud9.collection.DocnoMapping;

/* Note: different metrics are optimized separately */

public class Gov2_CIKM2010_Desc_Joint {

	private static final Logger sLogger = Logger.getLogger(Gov2_CIKM2010_Desc_Joint.class);

	private static String[] x10_rawAP = new String[] {
		"776","0.1739","777","0.2553","778","0.1344","779","0.3023","780","0.1918",
                "781","0.2798","782","0.6481","783","0.0944","784","0.4018","785","0.5398",
                "786","0.2709","787","0.5653","788","0.5293","789","0.2570","790","0.1515",
                "791","0.4063","792","0.1711","793","0.2605","794","0.1332","795","0.0230",
                "796","0.5233","797","0.4945","798","0.2494","799","0.1989","800","0.1836",
                "801","0.5611","802","0.3164","803","0.0002","804","0.5204","805","0.1835",
                "806","0.0665","807","0.5755","808","0.5792","809","0.1618","810","0.3156",
                "811","0.0617","812","0.2263","813","0.3073","814","0.4424","815","0.1383",
                "816","0.8137","817","0.2609","818","0.4388","819","0.4090","820","0.7493",
                "821","0.2554","822","0.1509","823","0.5505","824","0.3312","825","0.1146",
                "826","0.3483","827","0.4116","828","0.3497","829","0.0741","830","0.1193",
                "831","0.1046","832","0.0302","833","0.4219","834","0.5372","835","0.5376",
                "836","0.1787","837","0.0404","838","0.4333","839","0.4117","840","0.2478",
                "841","0.3720","842","0.2682","843","0.5295","844","0.3202","845","0.3146",
                "846","0.1186","847","0.3963","848","0.0532","849","0.1956","850","0.2202"};

	private static String[] x15_rawAP = new String[] {
		"776","0.2341","777","0.3437","778","0.1745","779","0.2959","780","0.1853",
                "781","0.3028","782","0.6157","783","0.0881","784","0.4625","785","0.5499",
                "786","0.2921","787","0.5710","788","0.5041","789","0.2590","790","0.1140",
                "791","0.4372","792","0.1818","793","0.2524","794","0.1139","795","0.0200",
                "796","0.5191","797","0.5147","798","0.2205","799","0.1716","800","0.1841",
                "801","0.6277","802","0.1521","803","0.0011","804","0.5424","805","0.1827",
                "806","0.0502","807","0.5319","808","0.5152","809","0.1768","810","0.2923",
                "811","0.0377","812","0.5016","813","0.2998","814","0.4867","815","0.1882",
                "816","0.6566","817","0.2693","818","0.4523","819","0.3093","820","0.7584",
                "821","0.1725","822","0.1045","823","0.5589","824","0.3829","825","0.1009",
                "826","0.3479","827","0.4455","828","0.4167","829","0.1161","830","0.1593",
                "831","0.0628","832","0.0493","833","0.3385","834","0.5054","835","0.5071",
                "836","0.2052","837","0.0251","838","0.4492","839","0.4113","840","0.2267",
                "841","0.5123","842","0.3398","843","0.6038","844","0.2462","845","0.3260",
                "846","0.0900","847","0.4339","848","0.0508","849","0.1105","850","0.2309"};

	private static String[] x20_rawAP = new String[] {
		"776","0.2312","777","0.3551","778","0.2479","779","0.2557","780","0.0081",
                "781","0.3984","782","0.5855","783","0.0832","784","0.4535","785","0.5600",
                "786","0.2891","787","0.5379","788","0.5539","789","0.2597","790","0.1177",
                "791","0.3866","792","0.1650","793","0.2498","794","0.1167","795","0.0240",
                "796","0.5192","797","0.5302","798","0.2087","799","0.1834","800","0.1771",
                "801","0.5620","802","0.1745","803","0.0021","804","0.5469","805","0.1949",
                "806","0.0349","807","0.5279","808","0.5924","809","0.1551","810","0.3693",
                "811","0.0683","812","0.5078","813","0.3050","814","0.5451","815","0.1817",
                "816","0.6993","817","0.1852","818","0.2181","819","0.2473","820","0.7754",
                "821","0.2117","822","0.1098","823","0.5590","824","0.3888","825","0.1255",
                "826","0.3280","827","0.3817","828","0.4135","829","0.2856","830","0.1842",
                "831","0.0595","832","0.0458","833","0.3732","834","0.5830","835","0.5156",
                "836","0.1943","837","0.0334","838","0.5110","839","0.4772","840","0.2500",
                "841","0.4073","842","0.3466","843","0.5257","844","0.1552","845","0.4010",
                "846","0.1297","847","0.4395","848","0.0745","849","0.2477","850","0.2114"};

	private static String[] x25_rawAP = new String[] {
		"776","0.1801","777","0.2521","778","0.2062","779","0.2956","780","0.1306",
                "781","0.2218","782","0.5674","783","0.0559","784","0.4894","785","0.5945",
                "786","0.2883","787","0.5333","788","0.5717","789","0.2617","790","0.0804",
                "791","0.3752","792","0.1758","793","0.2641","794","0.1255","795","0.0244",
                "796","0.4828","797","0.5387","798","0.2200","799","0.1401","800","0.1672",
                "801","0.5987","802","0.1280","803","0.0017","804","0.5137","805","0.1941",
                "806","0.0629","807","0.5274","808","0.6409","809","0.1218","810","0.3523",
                "811","0.0633","812","0.5669","813","0.3056","814","0.5557","815","0.1549",
                "816","0.6770","817","0.1837","818","0.2426","819","0.2036","820","0.7860",
                "821","0.1908","822","0.1067","823","0.5597","824","0.3804","825","0.1414",
                "826","0.3527","827","0.3659","828","0.4242","829","0.2863","830","0.2284",
                "831","0.0804","832","0.0409","833","0.3402","834","0.5620","835","0.5309",
                "836","0.2366","837","0.0298","838","0.5258","839","0.4043","840","0.2506",
                "841","0.4176","842","0.3840","843","0.5658","844","0.2361","845","0.3735",
                "846","0.1567","847","0.2990","848","0.0790","849","0.3827","850","0.2234"};

	private static String[] x30_rawAP = new String[] {
		"776","0.2155","777","0.3167","778","0.2062","779","0.2433","780","0.1837",
                "781","0.3678","782","0.6026","783","0.0784","784","0.4848","785","0.6260",
                "786","0.2838","787","0.5318","788","0.5727","789","0.2628","790","0.0797",
                "791","0.4198","792","0.1485","793","0.3567","794","0.1170","795","0.0244",
                "796","0.5103","797","0.5379","798","0.1925","799","0.1605","800","0.1385",
                "801","0.6136","802","0.1647","803","0.0026","804","0.5343","805","0.1859",
                "806","0.0734","807","0.5409","808","0.7138","809","0.1506","810","0.4175",
                "811","0.0779","812","0.5699","813","0.3238","814","0.5753","815","0.1981",
                "816","0.6397","817","0.1445","818","0.3352","819","0.4535","820","0.7772",
                "821","0.2593","822","0.1034","823","0.5579","824","0.3719","825","0.1502",
                "826","0.3428","827","0.3992","828","0.4541","829","0.2530","830","0.1963",
                "831","0.0632","832","0.0459","833","0.3247","834","0.5771","835","0.5231",
                "836","0.2358","837","0.0208","838","0.4101","839","0.3502","840","0.2506",
                "841","0.5673","842","0.3830","843","0.5736","844","0.3105","845","0.3857",
                "846","0.1097","847","0.4301","848","0.0821","849","0.4537","850","0.2381"};

	private static String[] x35_rawAP = new String[] {
		"776","0.1739","777","0.3210","778","0.1497","779","0.2439","780","0.1852",
                "781","0.2001","782","0.5954","783","0.0467","784","0.4896","785","0.5583",
                "786","0.2830","787","0.5515","788","0.5753","789","0.2796","790","0.0785",
                "791","0.4267","792","0.1503","793","0.3566","794","0.1124","795","0.0266",
                "796","0.5131","797","0.5259","798","0.1790","799","0.0924","800","0.1493",
                "801","0.6073","802","0.3442","803","0.0038","804","0.5288","805","0.1863",
                "806","0.0777","807","0.6027","808","0.5586","809","0.1330","810","0.4042",
                "811","0.0713","812","0.5604","813","0.3405","814","0.5539","815","0.1642",
                "816","0.6354","817","0.2562","818","0.3413","819","0.4524","820","0.7686",
                "821","0.3255","822","0.1205","823","0.5633","824","0.3840","825","0.1818",
                "826","0.3498","827","0.3579","828","0.4535","829","0.2534","830","0.1948",
                "831","0.0587","832","0.0438","833","0.3082","834","0.5797","835","0.5216",
                "836","0.2244","837","0.0293","838","0.3690","839","0.4926","840","0.2638",
                "841","0.4002","842","0.3544","843","0.6012","844","0.3824","845","0.4030",
                "846","0.1775","847","0.4248","848","0.0801","849","0.4613","850","0.2346"};

	private static String[] x40_rawAP = new String[] {
		"776","0.2038","777","0.2873","778","0.1496","779","0.2403","780","0.0085",
                "781","0.3663","782","0.5795","783","0.0829","784","0.4896","785","0.6210",
                "786","0.2780","787","0.5684","788","0.6007","789","0.2978","790","0.1082",
                "791","0.4252","792","0.1680","793","0.3513","794","0.1194","795","0.0272",
                "796","0.5059","797","0.4862","798","0.1921","799","0.2097","800","0.1539",
                "801","0.5272","802","0.3797","803","0.0035","804","0.5428","805","0.1866",
                "806","0.0879","807","0.6086","808","0.7429","809","0.1781","810","0.4070",
                "811","0.0615","812","0.5234","813","0.3314","814","0.5652","815","0.2061",
                "816","0.6312","817","0.3690","818","0.3842","819","0.5567","820","0.7739",
                "821","0.3308","822","0.1117","823","0.5860","824","0.3164","825","0.1925",
                "826","0.3533","827","0.3813","828","0.3931","829","0.2644","830","0.1951",
                "831","0.0633","832","0.0379","833","0.3566","834","0.5500","835","0.5217",
                "836","0.2352","837","0.0278","838","0.5152","839","0.4324","840","0.2088",
                "841","0.5659","842","0.3562","843","0.5672","844","0.3761","845","0.4122",
                "846","0.1302","847","0.2716","848","0.0772","849","0.3997","850","0.1979"};

	private static String[] x45_rawAP = new String[] {
		"776","0.2213","777","0.2587","778","0.1906","779","0.3244","780","0.0347",
                "781","0.2157","782","0.6333","783","0.0709","784","0.4896","785","0.6244",
                "786","0.2632","787","0.5713","788","0.5561","789","0.2950","790","0.1294",
                "791","0.4650","792","0.1750","793","0.3513","794","0.1656","795","0.0265",
                "796","0.4465","797","0.4677","798","0.2257","799","0.2209","800","0.1859",
                "801","0.6152","802","0.3314","803","0.0020","804","0.5221","805","0.1878",
                "806","0.0800","807","0.6554","808","0.6009","809","0.1149","810","0.4070",
                "811","0.0486","812","0.3544","813","0.3255","814","0.5728","815","0.1869",
                "816","0.7643","817","0.3645","818","0.3424","819","0.4651","820","0.7846",
                "821","0.2853","822","0.1038","823","0.5643","824","0.2814","825","0.1929",
                "826","0.3665","827","0.4436","828","0.4552","829","0.2933","830","0.1958",
                "831","0.0369","832","0.0362","833","0.3566","834","0.5195","835","0.5346",
                "836","0.1952","837","0.0320","838","0.4874","839","0.5286","840","0.1686",
                "841","0.3640","842","0.3881","843","0.5981","844","0.4452","845","0.3363",
                "846","0.1618","847","0.4193","848","0.0462","849","0.4072","850","0.1890"};

	private static String[] x50_rawAP = new String[] {
		"776","0.2280","777","0.2657","778","0.1923","779","0.3393","780","0.0119",
                "781","0.2332","782","0.6333","783","0.0681","784","0.5214","785","0.6296",
                "786","0.3236","787","0.5713","788","0.5561","789","0.2714","790","0.1297",
                "791","0.4754","792","0.1710","793","0.3502","794","0.1497","795","0.0258",
                "796","0.5287","797","0.4876","798","0.2230","799","0.2209","800","0.2191",
                "801","0.6025","802","0.4074","803","0.0015","804","0.4863","805","0.1989",
                "806","0.0948","807","0.6432","808","0.6520","809","0.1287","810","0.4070",
                "811","0.0648","812","0.3683","813","0.3255","814","0.5804","815","0.1491",
                "816","0.7955","817","0.3679","818","0.3720","819","0.4651","820","0.7647",
                "821","0.3019","822","0.1218","823","0.5707","824","0.2804","825","0.1712",
                "826","0.3990","827","0.4339","828","0.4087","829","0.1329","830","0.1880",
                "831","0.0503","832","0.0323","833","0.3566","834","0.5296","835","0.5645",
                "836","0.1966","837","0.0459","838","0.3222","839","0.4219","840","0.1600",
                "841","0.3883","842","0.3741","843","0.5981","844","0.4014","845","0.3289",
                "846","0.1412","847","0.4255","848","0.0609","849","0.4072","850","0.1865"};

	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("joint-x1.0", new GroundTruth("joint-x1.0", Metric.AP, 75, x10_rawAP, 0.3067f));
		g.put("joint-x1.5", new GroundTruth("joint-x1.5", Metric.AP, 75, x15_rawAP, 0.3089f));
		g.put("joint-x2.0", new GroundTruth("joint-x2.0", Metric.AP, 75, x20_rawAP, 0.3115f));
		g.put("joint-x2.5", new GroundTruth("joint-x2.5", Metric.AP, 75, x25_rawAP, 0.3104f));
		g.put("joint-x3.0", new GroundTruth("joint-x3.0", Metric.AP, 75, x30_rawAP, 0.3277f));
		g.put("joint-x3.5", new GroundTruth("joint-x3.5", Metric.AP, 75, x35_rawAP, 0.3260f));
		g.put("joint-x4.0", new GroundTruth("joint-x4.0", Metric.AP, 75, x40_rawAP, 0.3335f));
		g.put("joint-x4.5", new GroundTruth("joint-x4.5", Metric.AP, 75, x45_rawAP, 0.3302f));
		g.put("joint-x5.0", new GroundTruth("joint-x5.0", Metric.AP, 75, x50_rawAP, 0.3294f));

		Qrels qrels = new Qrels("data/gov2/qrels.gov2.all");

    String[] params = new String[] {
            "data/gov2/run.gov2.CIKM2010.desc.joint.xml",
            "data/gov2/gov2.desc.776-850" };

		FileSystem fs = FileSystem.getLocal(new Configuration());

		BatchQueryRunner qr = new BatchQueryRunner(params, fs);

		long start = System.currentTimeMillis();
		qr.runQueries();
		long end = System.currentTimeMillis();

		sLogger.info("Total query time: " + (end - start) + "ms");

		DocnoMapping mapping = qr.getDocnoMapping();

		for (String model : qr.getModels()) {
			sLogger.info("Verifying results of model \"" + model + "\"");

			Map<String, Accumulator[]> results = qr.getResults(model);
			g.get(model).verify(results, mapping, qrels);

			sLogger.info("Done!");
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Gov2_CIKM2010_Desc_Joint.class);
	}
}