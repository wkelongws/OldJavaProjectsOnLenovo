import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.SMO;
import weka.classifiers.meta.AdaBoostM1;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;

public class CrashPredMachineLearning {

	public static void main(String[] args) throws IOException {
		
//		String path_csv ="C:/Shuo Wang/study/study/Projects & Papers/Project - Active Traffic Management/Active Traffic Management 2014/reduced data/Data_for speed_crash_analysis/CSVdata/";
//		String output ="C:/Shuo Wang/study/study/Projects & Papers/Project - Active Traffic Management/Active Traffic Management 2014/reduced data/Data_for speed_crash_analysis/AUC/";
		String path_csv ="C:/Users/shuowang/Desktop/Data_for speed_crash_analysis/CSV/";
		String output ="C:/Users/shuowang/Desktop/WaveDownload/AUC/";
		DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");		
		String files_out;

		File csv_folder = new File(path_csv);

		File [] list_Files = csv_folder.listFiles(new FilenameFilter(){

			@Override
			public boolean accept(File folder, String name){
			return name.endsWith(".csv");				
			}
		});
		//list_Files.length
		for (int ab=0;ab<list_Files.length;ab++){
			files_out = list_Files[ab].getName();			
			String inputfile = path_csv+files_out;
			String a=files_out.split(".csv")[0];
			String Ratio=a.split("Ratio")[1];
			String a1=a.split("Ratio")[0];
			String Dur=a1.split("Dur")[1];
			String a2=a1.split("Dur")[0];
			String Off=a2.split("Off")[1];
			String a3=a2.split("Off")[0];
			String Down=a3.split("Down")[1];
			String a4=a3.split("Down")[0];
			String Up=a4.split("Up")[1];			
			
			PrintStream out = new PrintStream(new FileOutputStream(output +"AUCresults"+".txt", true));
			System.setOut(out);			
			try{
				// load csv data
				CSVLoader loader = new CSVLoader();
				loader.setOptions(weka.core.Utils.splitOptions("-H"));
			    loader.setSource(new File(inputfile));
			    Instances data = loader.getDataSet();
			    data.setClassIndex(data.numAttributes() - 1);
			    // numeric to nominal filter
			    NumericToNominal filter = new NumericToNominal();                         
			    filter.setOptions(weka.core.Utils.splitOptions("-R last"));                      
			    filter.setInputFormat(data);                         
			    Instances Data = Filter.useFilter(data, filter); 
			    
			    NaiveBayes NB = new NaiveBayes();
			    NB.setOptions(weka.core.Utils.splitOptions("")); 
			    NB.buildClassifier(Data); 
			    Evaluation eval_NB = new Evaluation(Data);
			    eval_NB.crossValidateModel(NB, Data, 5, new Random(1));
			    double AUC_NB=eval_NB.areaUnderROC(0);
			    Date time11 = Calendar.getInstance().getTime();
			    String time1 = dateFormat.format(time11);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+1+","+0+","+AUC_NB+","+time1);
			    
			    J48 tree = new J48();        
			    tree.setOptions(weka.core.Utils.splitOptions("")); 
			    tree.buildClassifier(Data);  
			    Evaluation eval_tree = new Evaluation(Data);
			    eval_tree.crossValidateModel(tree, Data, 5, new Random(1));
			    double AUC_tree=eval_tree.areaUnderROC(0);
			    Date time21 = Calendar.getInstance().getTime();
			    String time2 = dateFormat.format(time21);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+2+","+0+","+AUC_tree+","+time2);
			    
			    SMO SVM = new SMO();        
			    SVM.setOptions(weka.core.Utils.splitOptions("")); 
			    SVM.buildClassifier(Data);  
			    Evaluation eval_SVM = new Evaluation(Data);
			    eval_SVM.crossValidateModel(SVM, Data, 5, new Random(1));
			    double AUC_SVM=eval_SVM.areaUnderROC(0);
			    Date time31 = Calendar.getInstance().getTime();
			    String time3 = dateFormat.format(time31);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+3+","+0+","+AUC_SVM+","+time3);
			    
			    Logistic LR = new Logistic();        
			    LR.setOptions(weka.core.Utils.splitOptions("")); 
			    LR.buildClassifier(Data);  
			    Evaluation eval_LR = new Evaluation(Data);
			    eval_LR.crossValidateModel(LR, Data, 5, new Random(1));
			    double AUC_LR=eval_LR.areaUnderROC(0);
			    Date time41 = Calendar.getInstance().getTime();
			    String time4 = dateFormat.format(time41);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+4+","+0+","+AUC_LR+","+time4);
			    
			    AdaBoostM1 adaNB = new AdaBoostM1();        
			    adaNB.setOptions(weka.core.Utils.splitOptions("-I 5 -W weka.classifiers.bayes.NaiveBayes")); 
			    adaNB.buildClassifier(Data);  
			    Evaluation eval_adaNB = new Evaluation(Data);
			    eval_adaNB.crossValidateModel(adaNB, Data, 5, new Random(1));
			    double AUC_adaNB=eval_adaNB.areaUnderROC(0);
			    Date time51 = Calendar.getInstance().getTime();
			    String time5 = dateFormat.format(time51);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+5+","+1+","+AUC_adaNB+","+time5);
			    
			    AdaBoostM1 adaJ48 = new AdaBoostM1();        
			    adaJ48.setOptions(weka.core.Utils.splitOptions("-I 5 -W weka.classifiers.trees.J48")); 
			    adaJ48.buildClassifier(Data);  
			    Evaluation eval_adaJ48 = new Evaluation(Data);
			    eval_adaJ48.crossValidateModel(adaJ48, Data, 5, new Random(1));
			    double AUC_adaJ48=eval_adaJ48.areaUnderROC(0);
			    Date time61 = Calendar.getInstance().getTime();
			    String time6 = dateFormat.format(time61);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+6+","+1+","+AUC_adaJ48+","+time6);
			    
			    AdaBoostM1 adaSVM = new AdaBoostM1();        
			    adaSVM.setOptions(weka.core.Utils.splitOptions("-I 5 -W weka.classifiers.functions.SMO")); 
			    adaSVM.buildClassifier(Data);  
			    Evaluation eval_adaSVM = new Evaluation(Data);
			    eval_adaSVM.crossValidateModel(adaSVM, Data, 5, new Random(1));
			    double AUC_adaSVM=eval_adaSVM.areaUnderROC(0);
			    Date time71 = Calendar.getInstance().getTime();
			    String time7 = dateFormat.format(time71);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+7+","+1+","+AUC_adaSVM+","+time7);
			    
			    AdaBoostM1 adaLR = new AdaBoostM1();        
			    adaLR.setOptions(weka.core.Utils.splitOptions("-I 5 -W weka.classifiers.functions.Logistic")); 
			    adaLR.buildClassifier(Data);  
			    Evaluation eval_adaLR = new Evaluation(Data);
			    eval_adaLR.crossValidateModel(adaLR, Data, 5, new Random(1));
			    double AUC_adaLR=eval_adaLR.areaUnderROC(0);
			    Date time81 = Calendar.getInstance().getTime();
			    String time8 = dateFormat.format(time81);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+8+","+1+","+AUC_adaLR+","+time8);
			    
			    SMO SVMgk = new SMO();        
			    SVMgk.setOptions(weka.core.Utils.splitOptions("-K \"weka.classifiers.functions.supportVector.RBFKernel -G 0.01\"")); 
			    SVMgk.buildClassifier(Data);  
			    Evaluation eval_SVMgk = new Evaluation(Data);
			    eval_SVMgk.crossValidateModel(SVMgk, Data, 5, new Random(1));
			    double AUC_SVMgk=eval_SVMgk.areaUnderROC(0);
			    Date time91 = Calendar.getInstance().getTime();
			    String time9 = dateFormat.format(time91);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+9+","+2+","+AUC_SVMgk+","+time9);
			    
			    MultilayerPerceptron NN = new MultilayerPerceptron();        
			    NN.setOptions(weka.core.Utils.splitOptions("-N 200 -V 20 -H \"10,10,10\"")); 
			    NN.buildClassifier(Data);  
			    Evaluation eval_NN = new Evaluation(Data);
			    eval_NN.crossValidateModel(NN, Data, 5, new Random(1));
			    double AUC_NN=eval_NN.areaUnderROC(0);
			    Date time101 = Calendar.getInstance().getTime();
			    String time10 = dateFormat.format(time101);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+10+","+2+","+AUC_NN+","+time10);
			    
			    
			    PrintStream out1 = new PrintStream(new FileOutputStream(output +"AUCresults_duplicate"+".txt", true));
				System.setOut(out1);
				System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+1+","+0+","+AUC_NB+","+time1);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+2+","+0+","+AUC_tree+","+time2);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+3+","+0+","+AUC_SVM+","+time3);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+4+","+0+","+AUC_LR+","+time4);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+5+","+1+","+AUC_adaNB+","+time5);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+6+","+1+","+AUC_adaJ48+","+time6);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+7+","+1+","+AUC_adaSVM+","+time7);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+8+","+1+","+AUC_adaLR+","+time8);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+9+","+2+","+AUC_SVMgk+","+time9);
			    System.out.println(Up+","+Down+","+Off+","+Dur+","+Ratio+","+10+","+2+","+AUC_NN+","+time10);
			    
			}catch (Exception e) {
				e.printStackTrace();
		    }			
		}

	}

/*	public static double AUC(Instances data,Classifier model, String options) throws Exception {
		model.setOptions(weka.core.Utils.splitOptions(options));		
		return 0;		
	}*/
}
