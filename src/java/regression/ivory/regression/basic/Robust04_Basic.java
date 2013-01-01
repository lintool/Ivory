package ivory.regression.basic;

import static org.junit.Assert.assertEquals;
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.Map;
import java.util.Set;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class Robust04_Basic {
  private static final Logger LOG = Logger.getLogger(Robust04_Basic.class);

  static final ImmutableMap<String, Float> DIR_BASE_AP = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.4648f).put("602", 0.2787f).put("603", 0.2931f).put("604", 0.8289f).put("605", 0.0758f)
      .put("606", 0.4768f).put("607", 0.2038f).put("608", 0.0548f).put("609", 0.3040f).put("610", 0.0245f)
      .put("611", 0.2730f).put("612", 0.4695f).put("613", 0.2278f).put("614", 0.2014f).put("615", 0.1071f)
      .put("616", 0.7291f).put("617", 0.2573f).put("618", 0.2135f).put("619", 0.5546f).put("620", 0.0671f)
      .put("621", 0.3175f).put("622", 0.0349f).put("623", 0.3311f).put("624", 0.2460f).put("625", 0.0247f)
      .put("626", 0.1542f).put("627", 0.0140f).put("628", 0.2397f).put("629", 0.1319f).put("630", 0.6110f)
      .put("631", 0.1560f).put("632", 0.2665f).put("633", 0.4968f).put("634", 0.7553f).put("635", 0.5210f)
      .put("636", 0.1321f).put("637", 0.4508f).put("638", 0.0414f).put("639", 0.1334f).put("640", 0.3590f)
      .put("641", 0.3169f).put("642", 0.1531f).put("643", 0.4792f).put("644", 0.2338f).put("645", 0.5992f)
      .put("646", 0.3064f).put("647", 0.2310f).put("648", 0.1672f).put("649", 0.7222f).put("650", 0.0874f)
      .put("651", 0.0574f).put("652", 0.3183f).put("653", 0.5799f).put("654", 0.4083f).put("655", 0.0014f)
      .put("656", 0.5132f).put("657", 0.4083f).put("658", 0.1280f).put("659", 0.4606f).put("660", 0.6591f)
      .put("661", 0.5919f).put("662", 0.6254f).put("663", 0.4044f).put("664", 0.3955f).put("665", 0.2048f)
      .put("666", 0.0084f).put("667", 0.3518f).put("668", 0.3408f).put("669", 0.1557f).put("670", 0.1291f)
      .put("671", 0.3049f).put("672", 0.0000f).put("673", 0.3175f).put("674", 0.1371f).put("675", 0.2941f)
      .put("676", 0.2827f).put("677", 0.8928f).put("678", 0.2102f).put("679", 0.8833f).put("680", 0.0756f)
      .put("681", 0.3877f).put("682", 0.2516f).put("683", 0.0273f).put("684", 0.0918f).put("685", 0.2809f)
      .put("686", 0.2515f).put("687", 0.2149f).put("688", 0.1168f).put("689", 0.0060f).put("690", 0.0047f)
      .put("691", 0.3403f).put("692", 0.4541f).put("693", 0.3279f).put("694", 0.4762f).put("695", 0.2949f)
      .put("696", 0.2975f).put("697", 0.1600f).put("698", 0.4824f).put("699", 0.4594f).put("700", 0.4381f)
      .build();

  static final ImmutableMap<String, Float> DIR_BASE_P10 = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.3000f).put("602", 0.3000f).put("603", 0.2000f).put("604", 0.6000f).put("605", 0.1000f)
      .put("606", 0.5000f).put("607", 0.3000f).put("608", 0.1000f).put("609", 0.6000f).put("610", 0.0000f)
      .put("611", 0.5000f).put("612", 0.7000f).put("613", 0.5000f).put("614", 0.2000f).put("615", 0.2000f)
      .put("616", 1.0000f).put("617", 0.5000f).put("618", 0.3000f).put("619", 0.7000f).put("620", 0.1000f)
      .put("621", 0.7000f).put("622", 0.0000f).put("623", 0.6000f).put("624", 0.4000f).put("625", 0.1000f)
      .put("626", 0.1000f).put("627", 0.1000f).put("628", 0.4000f).put("629", 0.2000f).put("630", 0.3000f)
      .put("631", 0.2000f).put("632", 0.6000f).put("633", 1.0000f).put("634", 0.8000f).put("635", 0.8000f)
      .put("636", 0.1000f).put("637", 0.7000f).put("638", 0.2000f).put("639", 0.2000f).put("640", 0.6000f)
      .put("641", 0.5000f).put("642", 0.2000f).put("643", 0.4000f).put("644", 0.3000f).put("645", 0.9000f)
      .put("646", 0.4000f).put("647", 0.6000f).put("648", 0.5000f).put("649", 1.0000f).put("650", 0.2000f)
      .put("651", 0.2000f).put("652", 0.7000f).put("653", 0.6000f).put("654", 1.0000f).put("655", 0.0000f)
      .put("656", 0.7000f).put("657", 0.6000f).put("658", 0.3000f).put("659", 0.5000f).put("660", 0.9000f)
      .put("661", 0.8000f).put("662", 0.8000f).put("663", 0.5000f).put("664", 0.4000f).put("665", 0.4000f)
      .put("666", 0.0000f).put("667", 0.8000f).put("668", 0.5000f).put("669", 0.2000f).put("670", 0.3000f)
      .put("671", 0.5000f).put("672", 0.0000f).put("673", 0.5000f).put("674", 0.2000f).put("675", 0.5000f)
      .put("676", 0.3000f).put("677", 0.8000f).put("678", 0.3000f).put("679", 0.6000f).put("680", 0.2000f)
      .put("681", 0.5000f).put("682", 0.5000f).put("683", 0.3000f).put("684", 0.3000f).put("685", 0.4000f)
      .put("686", 0.4000f).put("687", 0.7000f).put("688", 0.3000f).put("689", 0.0000f).put("690", 0.0000f)
      .put("691", 0.5000f).put("692", 0.7000f).put("693", 0.3000f).put("694", 0.5000f).put("695", 0.9000f)
      .put("696", 0.6000f).put("697", 0.3000f).put("698", 0.3000f).put("699", 0.7000f).put("700", 0.7000f)
      .build();

  static final ImmutableMap<String, Float> DIR_SD_AP = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.5135f).put("602", 0.2988f).put("603", 0.2670f).put("604", 0.8385f).put("605", 0.0657f)
      .put("606", 0.5350f).put("607", 0.2260f).put("608", 0.0721f).put("609", 0.3058f).put("610", 0.0293f)
      .put("611", 0.2704f).put("612", 0.4854f).put("613", 0.2473f).put("614", 0.2059f).put("615", 0.0774f)
      .put("616", 0.7315f).put("617", 0.2576f).put("618", 0.2108f).put("619", 0.5605f).put("620", 0.0708f)
      .put("621", 0.3581f).put("622", 0.0776f).put("623", 0.3116f).put("624", 0.2599f).put("625", 0.0250f)
      .put("626", 0.1527f).put("627", 0.0188f).put("628", 0.2306f).put("629", 0.1923f).put("630", 0.7208f)
      .put("631", 0.1752f).put("632", 0.2267f).put("633", 0.4989f).put("634", 0.7522f).put("635", 0.5822f)
      .put("636", 0.1637f).put("637", 0.5026f).put("638", 0.0599f).put("639", 0.1473f).put("640", 0.3669f)
      .put("641", 0.3038f).put("642", 0.1601f).put("643", 0.4822f).put("644", 0.2329f).put("645", 0.5984f)
      .put("646", 0.3294f).put("647", 0.2360f).put("648", 0.3081f).put("649", 0.7009f).put("650", 0.0865f)
      .put("651", 0.0537f).put("652", 0.3186f).put("653", 0.5825f).put("654", 0.4255f).put("655", 0.0012f)
      .put("656", 0.5392f).put("657", 0.4574f).put("658", 0.1288f).put("659", 0.3330f).put("660", 0.6569f)
      .put("661", 0.6015f).put("662", 0.6263f).put("663", 0.4571f).put("664", 0.4658f).put("665", 0.2169f)
      .put("666", 0.0138f).put("667", 0.3590f).put("668", 0.3557f).put("669", 0.1582f).put("670", 0.1325f)
      .put("671", 0.3708f).put("672", 0.0000f).put("673", 0.3167f).put("674", 0.1359f).put("675", 0.3263f)
      .put("676", 0.2828f).put("677", 0.8723f).put("678", 0.2333f).put("679", 0.8972f).put("680", 0.0956f)
      .put("681", 0.5040f).put("682", 0.2648f).put("683", 0.0237f).put("684", 0.1280f).put("685", 0.2288f)
      .put("686", 0.2304f).put("687", 0.2306f).put("688", 0.1193f).put("689", 0.0121f).put("690", 0.0067f)
      .put("691", 0.3528f).put("692", 0.4736f).put("693", 0.3257f).put("694", 0.4725f).put("695", 0.2682f)
      .put("696", 0.2945f).put("697", 0.1693f).put("698", 0.4848f).put("699", 0.5331f).put("700", 0.5569f)
      .build();

  static final ImmutableMap<String, Float> DIR_SD_P10 = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.3000f).put("602", 0.3000f).put("603", 0.2000f).put("604", 0.6000f).put("605", 0.2000f)
      .put("606", 0.5000f).put("607", 0.3000f).put("608", 0.1000f).put("609", 0.6000f).put("610", 0.0000f)
      .put("611", 0.5000f).put("612", 0.6000f).put("613", 0.5000f).put("614", 0.3000f).put("615", 0.1000f)
      .put("616", 0.9000f).put("617", 0.5000f).put("618", 0.3000f).put("619", 0.6000f).put("620", 0.1000f)
      .put("621", 0.8000f).put("622", 0.1000f).put("623", 0.6000f).put("624", 0.4000f).put("625", 0.1000f)
      .put("626", 0.1000f).put("627", 0.1000f).put("628", 0.4000f).put("629", 0.4000f).put("630", 0.3000f)
      .put("631", 0.1000f).put("632", 0.6000f).put("633", 1.0000f).put("634", 0.8000f).put("635", 0.8000f)
      .put("636", 0.3000f).put("637", 0.6000f).put("638", 0.2000f).put("639", 0.2000f).put("640", 0.6000f)
      .put("641", 0.5000f).put("642", 0.2000f).put("643", 0.4000f).put("644", 0.2000f).put("645", 0.9000f)
      .put("646", 0.4000f).put("647", 0.6000f).put("648", 0.6000f).put("649", 1.0000f).put("650", 0.2000f)
      .put("651", 0.2000f).put("652", 0.7000f).put("653", 0.6000f).put("654", 1.0000f).put("655", 0.0000f)
      .put("656", 0.7000f).put("657", 0.6000f).put("658", 0.3000f).put("659", 0.4000f).put("660", 0.9000f)
      .put("661", 0.8000f).put("662", 0.8000f).put("663", 0.5000f).put("664", 0.6000f).put("665", 0.3000f)
      .put("666", 0.0000f).put("667", 0.8000f).put("668", 0.5000f).put("669", 0.1000f).put("670", 0.3000f)
      .put("671", 0.5000f).put("672", 0.0000f).put("673", 0.6000f).put("674", 0.1000f).put("675", 0.5000f)
      .put("676", 0.2000f).put("677", 0.7000f).put("678", 0.4000f).put("679", 0.6000f).put("680", 0.3000f)
      .put("681", 0.6000f).put("682", 0.5000f).put("683", 0.4000f).put("684", 0.3000f).put("685", 0.3000f)
      .put("686", 0.4000f).put("687", 0.8000f).put("688", 0.5000f).put("689", 0.0000f).put("690", 0.0000f)
      .put("691", 0.5000f).put("692", 0.7000f).put("693", 0.5000f).put("694", 0.5000f).put("695", 0.8000f)
      .put("696", 0.6000f).put("697", 0.3000f).put("698", 0.3000f).put("699", 0.7000f).put("700", 0.7000f)
      .build();

  static final ImmutableMap<String, Float> DIR_FD_AP = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.6646f).put("602", 0.2959f).put("603", 0.2887f).put("604", 0.8372f).put("605", 0.0675f)
      .put("606", 0.5663f).put("607", 0.2200f).put("608", 0.0918f).put("609", 0.3163f).put("610", 0.0249f)
      .put("611", 0.2672f).put("612", 0.4854f).put("613", 0.2481f).put("614", 0.2047f).put("615", 0.0611f)
      .put("616", 0.7315f).put("617", 0.2430f).put("618", 0.2012f).put("619", 0.5651f).put("620", 0.0750f)
      .put("621", 0.4290f).put("622", 0.0776f).put("623", 0.2936f).put("624", 0.2679f).put("625", 0.0253f)
      .put("626", 0.1527f).put("627", 0.0150f).put("628", 0.2306f).put("629", 0.1604f).put("630", 0.7870f)
      .put("631", 0.2420f).put("632", 0.2163f).put("633", 0.4989f).put("634", 0.7522f).put("635", 0.5746f)
      .put("636", 0.1605f).put("637", 0.5373f).put("638", 0.0599f).put("639", 0.1423f).put("640", 0.3604f)
      .put("641", 0.3075f).put("642", 0.1648f).put("643", 0.5010f).put("644", 0.2616f).put("645", 0.5984f)
      .put("646", 0.3440f).put("647", 0.2360f).put("648", 0.2918f).put("649", 0.7009f).put("650", 0.0813f)
      .put("651", 0.0537f).put("652", 0.3184f).put("653", 0.5750f).put("654", 0.4255f).put("655", 0.0012f)
      .put("656", 0.5515f).put("657", 0.4457f).put("658", 0.1288f).put("659", 0.3589f).put("660", 0.6600f)
      .put("661", 0.5970f).put("662", 0.6263f).put("663", 0.4663f).put("664", 0.5640f).put("665", 0.1965f)
      .put("666", 0.0137f).put("667", 0.3586f).put("668", 0.3557f).put("669", 0.1582f).put("670", 0.1325f)
      .put("671", 0.3678f).put("673", 0.3079f).put("672", 0.0000f).put("674", 0.1359f).put("675", 0.2996f)
      .put("676", 0.2828f).put("677", 0.8840f).put("678", 0.2260f).put("679", 0.8972f).put("680", 0.0983f)
      .put("681", 0.5156f).put("682", 0.2749f).put("683", 0.0237f).put("684", 0.1226f).put("685", 0.2444f)
      .put("686", 0.2450f).put("687", 0.2947f).put("688", 0.1135f).put("689", 0.0121f).put("690", 0.0062f)
      .put("691", 0.3413f).put("692", 0.4806f).put("693", 0.3385f).put("694", 0.4725f).put("695", 0.2460f)
      .put("696", 0.3029f).put("697", 0.1785f).put("698", 0.4851f).put("699", 0.5331f).put("700", 0.5569f)
      .build();

  static final ImmutableMap<String, Float> DIR_FD_P10 = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.3000f).put("602", 0.2000f).put("603", 0.2000f).put("604", 0.6000f).put("605", 0.3000f)
      .put("606", 0.5000f).put("607", 0.3000f).put("608", 0.1000f).put("609", 0.6000f).put("610", 0.0000f)
      .put("611", 0.5000f).put("612", 0.6000f).put("613", 0.5000f).put("614", 0.3000f).put("615", 0.0000f)
      .put("616", 0.9000f).put("617", 0.5000f).put("618", 0.3000f).put("619", 0.7000f).put("620", 0.1000f)
      .put("621", 0.9000f).put("622", 0.1000f).put("623", 0.5000f).put("624", 0.4000f).put("625", 0.1000f)
      .put("626", 0.1000f).put("627", 0.1000f).put("628", 0.4000f).put("629", 0.3000f).put("630", 0.3000f)
      .put("631", 0.6000f).put("632", 0.7000f).put("633", 1.0000f).put("634", 0.8000f).put("635", 0.8000f)
      .put("636", 0.2000f).put("637", 0.7000f).put("638", 0.2000f).put("639", 0.2000f).put("640", 0.6000f)
      .put("641", 0.7000f).put("642", 0.2000f).put("643", 0.4000f).put("644", 0.3000f).put("645", 0.9000f)
      .put("646", 0.4000f).put("647", 0.6000f).put("648", 0.6000f).put("649", 1.0000f).put("650", 0.1000f)
      .put("651", 0.2000f).put("652", 0.7000f).put("653", 0.5000f).put("654", 1.0000f).put("655", 0.0000f)
      .put("656", 0.7000f).put("657", 0.6000f).put("658", 0.3000f).put("659", 0.5000f).put("660", 0.9000f)
      .put("661", 0.8000f).put("662", 0.8000f).put("663", 0.6000f).put("664", 0.6000f).put("665", 0.3000f)
      .put("666", 0.0000f).put("667", 0.8000f).put("668", 0.5000f).put("669", 0.1000f).put("670", 0.3000f)
      .put("671", 0.5000f).put("672", 0.0000f).put("673", 0.6000f).put("674", 0.1000f).put("675", 0.5000f)
      .put("676", 0.2000f).put("677", 0.7000f).put("678", 0.4000f).put("679", 0.6000f).put("680", 0.3000f)
      .put("681", 0.6000f).put("682", 0.6000f).put("683", 0.4000f).put("684", 0.3000f).put("685", 0.3000f)
      .put("686", 0.3000f).put("687", 0.8000f).put("688", 0.5000f).put("689", 0.0000f).put("690", 0.0000f)
      .put("691", 0.5000f).put("692", 0.7000f).put("693", 0.5000f).put("694", 0.5000f).put("695", 0.8000f)
      .put("696", 0.7000f).put("697", 0.3000f).put("698", 0.3000f).put("699", 0.7000f).put("700", 0.7000f)
      .build();

  static final ImmutableMap<String, Float> BM25_BASE_AP = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.5441f).put("602", 0.2755f).put("603", 0.3273f).put("604", 0.8168f).put("605", 0.0713f)
      .put("606", 0.4982f).put("607", 0.1746f).put("608", 0.0645f).put("609", 0.3383f).put("610", 0.0170f)
      .put("611", 0.2175f).put("612", 0.5672f).put("613", 0.1909f).put("614", 0.1817f).put("615", 0.0715f)
      .put("616", 0.8164f).put("617", 0.2511f).put("618", 0.2063f).put("619", 0.5921f).put("620", 0.0799f)
      .put("621", 0.3915f).put("622", 0.0512f).put("623", 0.2854f).put("624", 0.2576f).put("625", 0.0276f)
      .put("626", 0.1267f).put("627", 0.0109f).put("628", 0.2449f).put("629", 0.1424f).put("630", 0.7024f)
      .put("631", 0.1751f).put("632", 0.2144f).put("633", 0.5022f).put("634", 0.7553f).put("635", 0.5225f)
      .put("636", 0.1364f).put("637", 0.4677f).put("638", 0.0375f).put("639", 0.1136f).put("640", 0.3195f)
      .put("641", 0.3270f).put("642", 0.1531f).put("643", 0.4771f).put("644", 0.2765f).put("645", 0.6010f)
      .put("646", 0.3262f).put("647", 0.2067f).put("648", 0.0824f).put("649", 0.7240f).put("650", 0.0986f)
      .put("651", 0.0521f).put("652", 0.3200f).put("653", 0.5812f).put("654", 0.1926f).put("655", 0.0017f)
      .put("656", 0.5236f).put("657", 0.3836f).put("658", 0.1365f).put("659", 0.2991f).put("660", 0.6603f)
      .put("661", 0.6059f).put("662", 0.6554f).put("663", 0.4316f).put("664", 0.5192f).put("665", 0.2212f)
      .put("666", 0.0060f).put("667", 0.3441f).put("668", 0.3811f).put("669", 0.1573f).put("670", 0.1019f)
      .put("671", 0.3157f).put("672", 0.0000f).put("673", 0.2703f).put("674", 0.1413f).put("675", 0.2656f)
      .put("676", 0.2868f).put("677", 0.9182f).put("678", 0.1751f).put("679", 0.8722f).put("680", 0.0615f)
      .put("681", 0.1297f).put("682", 0.2353f).put("683", 0.0316f).put("684", 0.0000f).put("685", 0.3065f)
      .put("686", 0.3040f).put("687", 0.2010f).put("688", 0.1059f).put("689", 0.0073f).put("690", 0.0046f)
      .put("691", 0.3800f).put("692", 0.4351f).put("693", 0.3423f).put("694", 0.4735f).put("695", 0.3155f)
      .put("696", 0.3306f).put("697", 0.1510f).put("698", 0.3768f).put("699", 0.4976f).put("700", 0.4617f)
      .build();

  static final ImmutableMap<String, Float> BM25_BASE_P10 = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.3000f).put("602", 0.3000f).put("603", 0.5000f).put("604", 0.6000f).put("605", 0.2000f)
      .put("606", 0.4000f).put("607", 0.3000f).put("608", 0.1000f).put("609", 0.6000f).put("610", 0.0000f)
      .put("611", 0.3000f).put("612", 0.7000f).put("613", 0.2000f).put("614", 0.1000f).put("615", 0.1000f)
      .put("616", 1.0000f).put("617", 0.6000f).put("618", 0.4000f).put("619", 0.8000f).put("620", 0.1000f)
      .put("621", 0.8000f).put("622", 0.1000f).put("623", 0.6000f).put("624", 0.4000f).put("625", 0.1000f)
      .put("626", 0.0000f).put("627", 0.1000f).put("628", 0.4000f).put("629", 0.2000f).put("630", 0.3000f)
      .put("631", 0.1000f).put("632", 0.6000f).put("633", 1.0000f).put("634", 0.8000f).put("635", 0.8000f)
      .put("636", 0.1000f).put("637", 0.8000f).put("638", 0.1000f).put("639", 0.2000f).put("640", 0.4000f)
      .put("641", 0.5000f).put("642", 0.2000f).put("643", 0.4000f).put("644", 0.3000f).put("645", 0.9000f)
      .put("646", 0.4000f).put("647", 0.5000f).put("648", 0.1000f).put("649", 1.0000f).put("650", 0.2000f)
      .put("651", 0.2000f).put("652", 0.8000f).put("653", 0.6000f).put("654", 0.4000f).put("655", 0.0000f)
      .put("656", 0.7000f).put("657", 0.5000f).put("658", 0.3000f).put("659", 0.2000f).put("660", 0.9000f)
      .put("661", 0.8000f).put("662", 0.9000f).put("663", 0.5000f).put("664", 0.6000f).put("665", 0.3000f)
      .put("666", 0.0000f).put("667", 0.7000f).put("668", 0.6000f).put("669", 0.1000f).put("670", 0.2000f)
      .put("671", 0.5000f).put("672", 0.0000f).put("673", 0.5000f).put("674", 0.2000f).put("675", 0.3000f)
      .put("676", 0.3000f).put("677", 0.8000f).put("678", 0.4000f).put("679", 0.6000f).put("680", 0.3000f)
      .put("681", 0.4000f).put("682", 0.6000f).put("683", 0.2000f).put("684", 0.0000f).put("685", 0.4000f)
      .put("686", 0.5000f).put("687", 0.8000f).put("688", 0.3000f).put("689", 0.0000f).put("690", 0.0000f)
      .put("691", 0.5000f).put("692", 0.7000f).put("693", 0.6000f).put("694", 0.6000f).put("695", 0.9000f)
      .put("696", 0.7000f).put("697", 0.3000f).put("698", 0.4000f).put("699", 0.7000f).put("700", 0.6000f)
      .build();

  static final ImmutableMap<String, Float> BM25_SD_AP = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.3367f).put("602", 0.2826f).put("603", 0.3152f).put("604", 0.8482f).put("605", 0.0688f)
      .put("606", 0.5647f).put("607", 0.2388f).put("608", 0.0832f).put("609", 0.3292f).put("610", 0.0262f)
      .put("611", 0.2474f).put("612", 0.5062f).put("613", 0.2137f).put("614", 0.1817f).put("615", 0.0556f)
      .put("616", 0.8186f).put("617", 0.2597f).put("618", 0.2128f).put("619", 0.5627f).put("620", 0.0333f)
      .put("621", 0.4792f).put("622", 0.2646f).put("623", 0.2387f).put("624", 0.2885f).put("625", 0.0275f)
      .put("626", 0.1257f).put("627", 0.0186f).put("628", 0.1838f).put("629", 0.1848f).put("630", 0.7794f)
      .put("631", 0.1962f).put("632", 0.1719f).put("633", 0.5117f).put("634", 0.7259f).put("635", 0.5711f)
      .put("636", 0.2654f).put("637", 0.5681f).put("638", 0.0624f).put("639", 0.1424f).put("640", 0.3534f)
      .put("641", 0.2754f).put("642", 0.1586f).put("643", 0.4527f).put("644", 0.2990f).put("645", 0.5870f)
      .put("646", 0.3925f).put("647", 0.2233f).put("648", 0.2946f).put("649", 0.6826f).put("650", 0.0900f)
      .put("651", 0.0585f).put("652", 0.3195f).put("653", 0.5636f).put("654", 0.3396f).put("655", 0.0014f)
      .put("656", 0.5427f).put("657", 0.4305f).put("658", 0.1467f).put("659", 0.2628f).put("660", 0.6435f)
      .put("661", 0.6096f).put("662", 0.6554f).put("663", 0.4750f).put("664", 0.6198f).put("665", 0.2055f)
      .put("666", 0.0143f).put("667", 0.3664f).put("668", 0.3592f).put("669", 0.1626f).put("670", 0.1100f)
      .put("671", 0.4210f).put("672", 0.0000f).put("673", 0.2742f).put("674", 0.1413f).put("675", 0.3369f)
      .put("676", 0.2855f).put("677", 0.8809f).put("678", 0.3005f).put("679", 0.7579f).put("680", 0.0653f)
      .put("681", 0.4063f).put("682", 0.2378f).put("683", 0.0300f).put("684", 0.1012f).put("685", 0.2813f)
      .put("686", 0.2550f).put("687", 0.2472f).put("688", 0.1162f).put("689", 0.0185f).put("690", 0.0097f)
      .put("691", 0.3823f).put("692", 0.4531f).put("693", 0.2793f).put("694", 0.4547f).put("695", 0.2756f)
      .put("696", 0.3198f).put("697", 0.1765f).put("698", 0.3823f).put("699", 0.5651f).put("700", 0.6554f)
      .build();

  static final ImmutableMap<String, Float> BM25_SD_P10 = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.4000f).put("602", 0.3000f).put("603", 0.4000f).put("604", 0.6000f).put("605", 0.2000f)
      .put("606", 0.4000f).put("607", 0.4000f).put("608", 0.1000f).put("609", 0.6000f).put("610", 0.0000f)
      .put("611", 0.5000f).put("612", 0.6000f).put("613", 0.2000f).put("614", 0.1000f).put("615", 0.0000f)
      .put("616", 1.0000f).put("617", 0.7000f).put("618", 0.3000f).put("619", 0.7000f).put("620", 0.0000f)
      .put("621", 0.8000f).put("622", 0.4000f).put("623", 0.5000f).put("624", 0.4000f).put("625", 0.1000f)
      .put("626", 0.0000f).put("627", 0.1000f).put("628", 0.4000f).put("629", 0.2000f).put("630", 0.3000f)
      .put("631", 0.1000f).put("632", 0.5000f).put("633", 1.0000f).put("634", 0.8000f).put("635", 0.7000f)
      .put("636", 0.4000f).put("637", 0.7000f).put("638", 0.3000f).put("639", 0.4000f).put("640", 0.5000f)
      .put("641", 0.5000f).put("642", 0.3000f).put("643", 0.4000f).put("644", 0.3000f).put("645", 0.9000f)
      .put("646", 0.4000f).put("647", 0.6000f).put("648", 0.7000f).put("649", 1.0000f).put("650", 0.1000f)
      .put("651", 0.1000f).put("652", 0.8000f).put("653", 0.6000f).put("654", 0.9000f).put("655", 0.0000f)
      .put("656", 0.7000f).put("657", 0.5000f).put("658", 0.3000f).put("659", 0.2000f).put("660", 0.9000f)
      .put("661", 0.9000f).put("662", 0.9000f).put("663", 0.6000f).put("664", 0.7000f).put("665", 0.3000f)
      .put("666", 0.0000f).put("667", 0.8000f).put("668", 0.5000f).put("669", 0.1000f).put("670", 0.3000f)
      .put("671", 0.5000f).put("672", 0.0000f).put("673", 0.5000f).put("674", 0.2000f).put("675", 0.6000f)
      .put("676", 0.2000f).put("677", 0.7000f).put("678", 0.5000f).put("679", 0.6000f).put("680", 0.2000f)
      .put("681", 0.5000f).put("682", 0.7000f).put("683", 0.4000f).put("684", 0.2000f).put("685", 0.4000f)
      .put("686", 0.4000f).put("687", 0.7000f).put("688", 0.3000f).put("689", 0.0000f).put("690", 0.0000f)
      .put("691", 0.4000f).put("692", 0.6000f).put("693", 0.5000f).put("694", 0.5000f).put("695", 0.8000f)
      .put("696", 0.7000f).put("697", 0.3000f).put("698", 0.3000f).put("699", 0.7000f).put("700", 0.8000f)
      .build();

  static final ImmutableMap<String, Float> BM25_FD_AP = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.6167f).put("602", 0.2798f).put("603", 0.3230f).put("604", 0.8317f).put("605", 0.0778f)
      .put("606", 0.5517f).put("607", 0.2029f).put("608", 0.0973f).put("609", 0.3458f).put("610", 0.0204f)
      .put("611", 0.2237f).put("612", 0.5359f).put("613", 0.2087f).put("614", 0.1817f).put("615", 0.0593f)
      .put("616", 0.8133f).put("617", 0.2437f).put("618", 0.2108f).put("619", 0.5797f).put("620", 0.0434f)
      .put("621", 0.5115f).put("622", 0.1996f).put("623", 0.2198f).put("624", 0.2874f).put("625", 0.0261f)
      .put("626", 0.1257f).put("627", 0.0142f).put("628", 0.2051f).put("629", 0.1523f).put("630", 0.7750f)
      .put("631", 0.2249f).put("632", 0.1956f).put("633", 0.5069f).put("634", 0.7259f).put("635", 0.5427f)
      .put("636", 0.2293f).put("637", 0.5653f).put("638", 0.0565f).put("639", 0.1202f).put("640", 0.3372f)
      .put("641", 0.3017f).put("642", 0.1606f).put("643", 0.4745f).put("644", 0.3150f).put("645", 0.6050f)
      .put("646", 0.3708f).put("647", 0.2190f).put("648", 0.2102f).put("649", 0.7001f).put("650", 0.0869f)
      .put("651", 0.0627f).put("652", 0.3195f).put("653", 0.5716f).put("654", 0.3061f).put("655", 0.0015f)
      .put("656", 0.5206f).put("657", 0.4070f).put("658", 0.1406f).put("659", 0.3341f).put("660", 0.6625f)
      .put("661", 0.6062f).put("662", 0.6554f).put("663", 0.4436f).put("664", 0.6615f).put("665", 0.2126f)
      .put("666", 0.0098f).put("667", 0.3461f).put("668", 0.3796f).put("669", 0.1631f).put("670", 0.1082f)
      .put("671", 0.3778f).put("672", 0.0000f).put("673", 0.2648f).put("674", 0.1413f).put("675", 0.2689f)
      .put("676", 0.2895f).put("677", 0.8888f).put("678", 0.2651f).put("679", 0.7802f).put("680", 0.0740f)
      .put("681", 0.2485f).put("682", 0.2308f).put("683", 0.0286f).put("684", 0.0752f).put("685", 0.2978f)
      .put("686", 0.2820f).put("687", 0.3280f).put("688", 0.1117f).put("689", 0.0169f).put("690", 0.0065f)
      .put("691", 0.3609f).put("692", 0.4630f).put("693", 0.3307f).put("694", 0.4590f).put("695", 0.2644f)
      .put("696", 0.3306f).put("697", 0.1764f).put("698", 0.4343f).put("699", 0.5765f).put("700", 0.6037f)
      .build();

  static final ImmutableMap<String, Float> BM25_FD_P10 = new ImmutableMap.Builder<String, Float>()
      .put("601", 0.3000f).put("602", 0.1000f).put("603", 0.4000f).put("604", 0.6000f).put("605", 0.3000f)
      .put("606", 0.4000f).put("607", 0.4000f).put("608", 0.1000f).put("609", 0.6000f).put("610", 0.0000f)
      .put("611", 0.3000f).put("612", 0.8000f).put("613", 0.2000f).put("614", 0.1000f).put("615", 0.0000f)
      .put("616", 1.0000f).put("617", 0.6000f).put("618", 0.3000f).put("619", 0.7000f).put("620", 0.1000f)
      .put("621", 0.8000f).put("622", 0.4000f).put("623", 0.5000f).put("624", 0.5000f).put("625", 0.1000f)
      .put("626", 0.0000f).put("627", 0.1000f).put("628", 0.4000f).put("629", 0.2000f).put("630", 0.3000f)
      .put("631", 0.4000f).put("632", 0.6000f).put("633", 1.0000f).put("634", 0.8000f).put("635", 0.8000f)
      .put("636", 0.4000f).put("637", 0.7000f).put("638", 0.3000f).put("639", 0.2000f).put("640", 0.5000f)
      .put("641", 0.5000f).put("642", 0.3000f).put("643", 0.4000f).put("644", 0.4000f).put("645", 0.9000f)
      .put("646", 0.4000f).put("647", 0.6000f).put("648", 0.5000f).put("649", 1.0000f).put("650", 0.0000f)
      .put("651", 0.3000f).put("652", 0.8000f).put("653", 0.6000f).put("654", 0.9000f).put("655", 0.0000f)
      .put("656", 0.7000f).put("657", 0.5000f).put("658", 0.3000f).put("659", 0.3000f).put("660", 0.9000f)
      .put("661", 0.9000f).put("662", 0.9000f).put("663", 0.6000f).put("664", 0.7000f).put("665", 0.3000f)
      .put("666", 0.0000f).put("667", 0.8000f).put("668", 0.5000f).put("669", 0.2000f).put("670", 0.3000f)
      .put("671", 0.5000f).put("672", 0.0000f).put("673", 0.5000f).put("674", 0.2000f).put("675", 0.4000f)
      .put("676", 0.3000f).put("677", 0.7000f).put("678", 0.4000f).put("679", 0.6000f).put("680", 0.4000f)
      .put("681", 0.5000f).put("682", 0.5000f).put("683", 0.4000f).put("684", 0.2000f).put("685", 0.4000f)
      .put("686", 0.4000f).put("687", 0.8000f).put("688", 0.3000f).put("689", 0.0000f).put("690", 0.0000f)
      .put("691", 0.4000f).put("692", 0.7000f).put("693", 0.5000f).put("694", 0.5000f).put("695", 0.7000f)
      .put("696", 0.8000f).put("697", 0.3000f).put("698", 0.3000f).put("699", 0.7000f).put("700", 0.8000f)
      .build();

  @Test
  public void runRegression() throws Exception {
    String[] params = new String[] {
            "data/trec/run.robust04.basic.xml",
            "data/trec/queries.robust04.xml" };

    FileSystem fs = FileSystem.getLocal(new Configuration());

    BatchQueryRunner qr = new BatchQueryRunner(params, fs);
    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();
    LOG.info("Total query time: " + (end - start) + "ms");

    verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/trec/qrels.robust04.noCRFR.txt"));
  }

  public static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {
    Map<String, Map<String, Float>> AllModelsAPScores = Maps.newHashMap();
    AllModelsAPScores.put("robust04-dir-base", DIR_BASE_AP);
    AllModelsAPScores.put("robust04-dir-sd", DIR_SD_AP);
    AllModelsAPScores.put("robust04-dir-fd", DIR_FD_AP);
    AllModelsAPScores.put("robust04-bm25-base", BM25_BASE_AP);
    AllModelsAPScores.put("robust04-bm25-sd", BM25_SD_AP);
    AllModelsAPScores.put("robust04-bm25-fd", BM25_FD_AP);

    Map<String, Map<String, Float>> AllModelsP10Scores = Maps.newHashMap();
    AllModelsP10Scores.put("robust04-dir-base", DIR_BASE_P10);
    AllModelsP10Scores.put("robust04-dir-sd", DIR_SD_P10);
    AllModelsP10Scores.put("robust04-dir-fd", DIR_FD_P10);
    AllModelsP10Scores.put("robust04-bm25-base", BM25_BASE_P10);
    AllModelsP10Scores.put("robust04-bm25-sd", BM25_SD_P10);
    AllModelsP10Scores.put("robust04-bm25-fd", BM25_FD_P10);
    
    for (String model : models) {
      LOG.info("Verifying results of model \"" + model + "\"");
      verifyResults(model, results.get(model),
          AllModelsAPScores.get(model), AllModelsP10Scores.get(model), mapping, qrels);
      LOG.info("Done!");
    }
  }

  private static void verifyResults(String model, Map<String, Accumulator[]> results,
      Map<String, Float> apScores, Map<String, Float> p10Scores, DocnoMapping mapping,
      Qrels qrels) {
    float apSum = 0, p10Sum = 0;
    for (String qid : results.keySet()) {
      float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      apSum += ap;
      p10Sum += p10;

      LOG.info("verifying qid " + qid + " for model " + model);
      assertEquals(apScores.get(qid), ap, 10e-6);
      assertEquals(p10Scores.get(qid), p10, 10e-6);
    }

    // One topic didn't contain qrels, so trec_eval only picked up 99 topics.
    float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 99f);
    float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / 99f);

    if (model.equals("robust04-dir-base")) {
      assertEquals(0.3063, MAP, 10e-5);
      assertEquals(0.4424, P10Avg, 10e-5);
    } else if (model.equals("robust04-dir-sd")) {
      assertEquals(0.3194, MAP, 10e-5);
      assertEquals(0.4485, P10Avg, 10e-5);
    } else if (model.equals("robust04-dir-fd")) {
      assertEquals(0.3253, MAP, 10e-5);
      assertEquals(0.4576, P10Avg, 10e-5);
    } else if (model.equals("robust04-bm25-base")) {
      assertEquals(0.3033, MAP, 10e-5);
      assertEquals(0.4283, P10Avg, 10e-5);
    } else if (model.equals("robust04-bm25-sd")) {
      assertEquals(0.3212, MAP, 10e-5);
      assertEquals(0.4505, P10Avg, 10e-5);
    } else if (model.equals("robust04-bm25-fd")) {
      assertEquals(0.3213, MAP, 10e-5);
      assertEquals(0.4545, P10Avg, 10e-5);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Robust04_Basic.class);
  }
}
