package org.cloudsimplus.examples.TASimulation.Markov;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.*;
import java.lang.*;
import java.io.*;

import org.cloudsimplus.*;
// import org.cloudsimplus.cloudlets.Cloudlet;
// import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
// import org.cloudbus.cloudsim.Datacenter;
// import org.cloudbus.cloudsim.DatacenterBroker;
// import org.cloudbus.cloudsim.DatacenterCharacteristics;
// import org.cloudbus.cloudsim.Host;
// import org.cloudbus.cloudsim.Log;
// import org.cloudbus.cloudsim.Pe;
// import org.cloudbus.cloudsim.Storage;
// import org.cloudbus.cloudsim.UtilizationModel;
// import org.cloudbus.cloudsim.UtilizationModelFull;
// import org.cloudbus.cloudsim.Vm;
// import org.cloudbus.cloudsim.VmAllocationPolicySimple;
// import org.cloudbus.cloudsim.VmSchedulerTimeShared;
// import org.cloudbus.cloudsim.core.CloudSim;
// import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
// import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
// import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;


public class MarkovLoadBalancer{
	protected double Q;
	protected double alpha;
	protected double beta;
	protected double gamma;
	protected double rho;
	protected int ant;
	protected Random r;

	public List<Cloudlet> getAllocatedNewCloudlets(List<Cloudlet> newCloudlets,List<Vm> vmList, Integer initialCloudlets) throws FileNotFoundException{

        int i = 0;
        int currentDistribution[] = new int[vmList.size()];
        Map<String, BigDecimal> memo = new HashMap<>();
        Map<String, BigDecimal> memo2 = new HashMap<>();

        Double taskDistProb = 1 / (double) vmList.size(); // Task Distribution Probability per VM (b1k)
        int lk = initialCloudlets / vmList.size(); // Current Load VM

        int expProcCapLB = getExpProcCapLB(newCloudlets);
        Double maxComPow = getMaxComPow(vmList);
        List<Double> facProcCap = getFacProcCap(vmList, maxComPow);
        List<Double> maxProcCap = getMaxProcCap(expProcCapLB, facProcCap);
        List<Double> expProcCapVM = getExpProcCapVM(maxProcCap, lk);
        List<Double> distFac = getDistFac(expProcCapLB, expProcCapVM, taskDistProb);
        BigDecimal Fn = getFn(newCloudlets.size(), vmList.size(), i, currentDistribution, distFac, memo);
        BigDecimal Fn_1 = getFn_1(newCloudlets.size() - 1, vmList.size(), i, currentDistribution, distFac, memo2);

        MathContext mc = new MathContext(4, RoundingMode.HALF_UP);
        BigDecimal expUtilLB = Fn_1.divide(Fn, mc);

        Map<String, List<?>> values = getExpUtilVMs(vmList, expUtilLB, distFac);
        List<Integer> removeVM = (ArrayList<Integer>) values.get("removeVM"); 
        List<Double> expUtilVMs = (ArrayList<Double>) values.get("expUtilVMs");

        List<Double> ratio = getRatio(removeVM, expProcCapVM, expProcCapLB, vmList);
        Double loadBalanceFac = getLoadBalanceFac(vmList, expProcCapLB, ratio);
        List<Double> workloadPerVM = getWorkloadPerVM(loadBalanceFac, ratio, removeVM, vmList); 
        List<Double> expProcCapAfter = getExpProcCapAfter(vmList, expProcCapVM, workloadPerVM);
        List<Double> taskDistProbAfter = getTaskDistProbAfter(workloadPerVM, expProcCapLB);
        List<Double> expUtilVMAfter = getExpUtilVMAfter(expUtilLB,expProcCapLB,expProcCapAfter, taskDistProbAfter);

        // System.out.println("This is expProCapLB: " + expProcCapLB);
        // System.out.println("This is maxComPow: " + maxComPow);
        // System.out.println("This is facProcCap: " + facProcCap);
        // System.out.println("This is maxProcCap: " + maxProcCap);
        // System.out.println("This is expProcCapVM: " + expProcCapVM);
        // System.out.println("This is distFac: " + distFac);
        // System.out.println("This is Fn: " + Fn);
        // System.out.println("This is Fn_1: " + Fn_1);
        // System.out.println("This is expUtilLB: " + expUtilLB);
        // System.out.println("This is values: " + values);
        // System.out.println("This is ratio: " + ratio);
        // System.out.println("This is loadBalanceFac: " + loadBalanceFac);
        // System.out.println("This is workloadPerVM: " + workloadPerVM);
        // System.out.println("This is expProcCapAfter: " + expProcCapAfter); 
        // System.out.println("This is taskDistProbAfter: " + taskDistProbAfter); 
        // System.out.println("This is expUtilVMAfter: " + expUtilVMAfter); 
        // System.out.println(loadBalanceAnalysis(expUtilVMs, expUtilVMAfter, removeVM));

        List<Cloudlet> allocatedNewCloudlets = allocateNewCloudlets(workloadPerVM, newCloudlets, vmList, expUtilVMs);

        return allocatedNewCloudlets;
	}

    protected int getExpProcCapLB(List<Cloudlet> newCloudlets){
        // Expected Processing Capacity Lber
        // miu 1
        int expProcCapLB = 0;
        
        for (int i = 0; i < newCloudlets.size(); i++) {
            expProcCapLB += newCloudlets.get(i).getLength();
        }
        return expProcCapLB;
        // return newCloudlets.size();
    }

    protected Double getMaxComPow(List<Vm> vmList){
        // CPMax
        Double maxComPow = vmList.get(0).getMips();
        
        for (int i = 1; i < vmList.size(); i++) {
            if (vmList.get(i).getMips() > maxComPow) {
                maxComPow = vmList.get(i).getMips();
            }
        }
        return maxComPow;
    }

    protected List<Double> getFacProcCap(List<Vm> vmList, Double maxComPow){
        // Calculating factor that expresses the processing capacity of each VM proportionally to the maximum CP that exists among the VMs.
        // Mk
        List<Double> facProcCap = new ArrayList<>();
        
        for (int i = 0; i < vmList.size(); i++) {
            facProcCap.add(maxComPow / vmList.get(i).getMips());
        }

        return facProcCap;
    }

    protected List<Double> getMaxProcCap(int expProcCapLB, List<Double> facProcCap){
        // Calculating maximum processing capacity for each VM
        // Average Miu k
        List<Double> maxProcCap = new ArrayList<>();

        for (int i = 0; i < facProcCap.size(); i++) {
            maxProcCap.add(expProcCapLB / facProcCap.get(i));
        }

        return maxProcCap;
    }

    protected List<Double> getExpProcCapVM(List<Double> maxProcCap, int lk){
        // Calculating Expected Processing Capacity of VMk
        // Miu k
        List<Double> expProcCapVM = new ArrayList<>();
        
        for (int i = 0; i < maxProcCap.size(); i++) {
            Double value = maxProcCap.get(i) - lk;
            expProcCapVM.add(Math.round(value * 10.0) / 10.0);
        }
        return expProcCapVM;
    }

    protected List<Double> getDistFac(int expProcCapLB, List<Double> expProcCapVM, Double taskDistProb){
        // Calculating distribution factor between the LBer and VMk
        // yk
        List<Double> distFac = new ArrayList<>();

        for (int i = 0; i < expProcCapVM.size(); i++) {
            double result = Math.round((expProcCapLB/expProcCapVM.get(i))*taskDistProb * 100.0) / 100.0; 
            distFac.add(result);
        }
        return distFac;
    }

   protected BigDecimal getFn(int newCloudletsSize, int vmListSize, int currentVm, int currentDistribution[], List<Double> distFac, Map<String, BigDecimal> memo) {
    String memoKey = newCloudletsSize + "," + currentVm;
    if (memo.containsKey(memoKey)) {
        return memo.get(memoKey);
    }

    if (currentVm == vmListSize - 1) {
        currentDistribution[currentVm] = newCloudletsSize;
        BigDecimal result = BigDecimal.ONE;
        for (int i = 0; i < vmListSize; i++) {
            BigDecimal base = BigDecimal.valueOf(distFac.get(i));
            int exponent = currentDistribution[i];
            result = result.multiply(base.pow(exponent));
        }
        
        memo.put(memoKey, result);
        return result;
    }

    BigDecimal Fn = BigDecimal.ZERO;
    for (int i = 0; i <= newCloudletsSize; i++) {
        currentDistribution[currentVm] = i;
        Fn = Fn.add(getFn(newCloudletsSize - i, vmListSize, currentVm + 1, currentDistribution, distFac, memo));
    }
    
    memo.put(memoKey, Fn);
    return Fn;
}

protected BigDecimal getFn_1(int newCloudletsSize, int vmListSize, int currentVm, int currentDistribution[], List<Double> distFac, Map<String, BigDecimal> memo2) {
    String memoKey = newCloudletsSize + "," + currentVm;
    if (memo2.containsKey(memoKey)) {
        return memo2.get(memoKey);
    }

    if (currentVm == vmListSize - 1) {
        currentDistribution[currentVm] = newCloudletsSize;
        BigDecimal result = BigDecimal.ONE;
        for (int i = 0; i < vmListSize; i++) {
            BigDecimal base = BigDecimal.valueOf(distFac.get(i));
            int exponent = currentDistribution[i];
            result = result.multiply(base.pow(exponent));
        }
        
        memo2.put(memoKey, result);
        return result;
    }

    BigDecimal Fn_1 = BigDecimal.ZERO;
    for (int i = 0; i <= newCloudletsSize; i++) {
        currentDistribution[currentVm] = i;
        Fn_1 = Fn_1.add(getFn(newCloudletsSize - i, vmListSize, currentVm + 1, currentDistribution, distFac, memo2));
    }
    
    memo2.put(memoKey, Fn_1);
    return Fn_1;
}


//     protected BigDecimal getFn_1(int newCloudletsSize, int vmListSize, int currentVm, int currentDistribution[], List<Double> distFac, Map<String, BigDecimal> memo2) {
//     if (currentVm == vmListSize - 1) {
//         currentDistribution[currentVm] = newCloudletsSize;
//         BigDecimal result = BigDecimal.ONE;
//         for (int i = 0; i < vmListSize; i++) {
//             BigDecimal base = BigDecimal.valueOf(distFac.get(i));
//             int exponent = currentDistribution[i];
//             result = result.multiply(base.pow(exponent));
//         }

//         return result;
//     }

//     String memoKey = newCloudletsSize + "," + currentVm;
//     if (memo2.containsKey(memoKey)) {
//         return memo2.get(memoKey);
//     }

//     BigDecimal Fn_1 = BigDecimal.ZERO;
//     int taskRangeStart = currentVm == 0 ? 1 : 0;
//     int taskRangeEnd = newCloudletsSize + 1;
//     for (int i = taskRangeStart; i < taskRangeEnd; i++) {
//         currentDistribution[currentVm] = i;
//         Fn_1 = Fn_1.add(getFn_1(newCloudletsSize - i, vmListSize, currentVm + 1, currentDistribution, distFac, memo2));
//     }

//     memo2.put(memoKey, Fn_1);
//     return Fn_1;
// }

    protected Map<String, List<?>> getExpUtilVMs(List<Vm> vmList, BigDecimal expUtilLB, List<Double> distFac){
    Map<String, List<?>> values = new HashMap<>();
    List<Integer> removeVM = new ArrayList<>();
    List<BigDecimal> expUtilVMs = new ArrayList<>();

    for (int i = 0; i < vmList.size(); i++) {
        BigDecimal val = expUtilLB.multiply(BigDecimal.valueOf(distFac.get(i)));
        BigDecimal scaledVal = val.setScale(1, RoundingMode.HALF_UP);

        if(scaledVal.compareTo(BigDecimal.valueOf(0.9)) > 0){
            removeVM.add(i);
        }

        expUtilVMs.add(scaledVal);
    }
    values.put("removeVM", removeVM);
    values.put("expUtilVMs", expUtilVMs);
    return values;
}


    protected List<Double> getRatio(List<Integer> removeVM, List<Double> expProcCapVM, int expProcCapLB, List<Vm> vmList){
        // Calculating Ratio of Expected Processing capacity and total number tasks to be distributed during timeslot 
        // uk
        List<Double> ratio = new ArrayList<>();
        
        for (int i = 0; i < vmList.size(); i++) {
            if(removeVM.contains(i)){
                ratio.add(0.0);
                continue;
            }
            double value = expProcCapVM.get(i) / expProcCapLB;
            ratio.add(Math.round(value * 100.0) / 100.0);    
        }

        return ratio;
    }

    protected Double getLoadBalanceFac(List<Vm> vmList, int expProcCapLB, List<Double> ratio){
          
        Double TOT_Ratio = 0.0;

         for (int i = 0; i < vmList.size(); i++) {
            TOT_Ratio += ratio.get(i);   
        }
        Double loadBalanceFac = expProcCapLB / TOT_Ratio;
        
        return loadBalanceFac;
    }

    protected List<Double> getWorkloadPerVM(Double loadBalanceFac, List<Double> ratio,  List<Integer> removeVM, List<Vm> vmList) {
        List<Double> workloadPerVM = new ArrayList<>();

        // Calculating The workload per VM after distribution
        // lk*
        for (int i = 0; i < vmList.size(); i++) {
            if(removeVM.contains(i)){
                workloadPerVM.add(0.0);
                continue;
            }
            double value = loadBalanceFac*ratio.get(i);
            workloadPerVM.add(Math.round(value * 10.0) / 10.0);
        }
        return workloadPerVM;
    }

    protected List<Double> getExpProcCapAfter(List<Vm> vmList, List<Double> expProcCapVM, List<Double> workloadPerVM){
        List<Double> expProcCapAfter = new ArrayList<>();
        
        for (int i = 0; i < vmList.size(); i++) {
            expProcCapAfter.add(expProcCapVM.get(i)-workloadPerVM.get(i));
        }

        return expProcCapAfter;
    }

    protected List<Double> getTaskDistProbAfter(List<Double> workloadPerVM, int expProcCapLB){
        List<Double> taskDistProbAfter = new ArrayList<>();

        for(int i=0; i< workloadPerVM.size(); i++){
            Double value = workloadPerVM.get(i)/expProcCapLB;
            taskDistProbAfter.add(Math.round(value * 100) / 100.0);
        }
        return taskDistProbAfter;
    }

    protected List<Double> getExpUtilVMAfter(BigDecimal expUtilLB, int expProcCapLB, List<Double> expProcCapAfter, List<Double> taskDistProbAfter){
        List<Double> expUtilVMAfter = new ArrayList<>();

        for (int i = 0; i < taskDistProbAfter.size(); i++) {

            BigDecimal term = expUtilLB.multiply(BigDecimal.valueOf(expProcCapLB / expProcCapAfter.get(i))).multiply(BigDecimal.valueOf(taskDistProbAfter.get(i)));
            term = term.setScale(1, RoundingMode.HALF_UP);
            expUtilVMAfter.add(term.doubleValue());
        }
        return expUtilVMAfter;
    }

    protected boolean loadBalanceAnalysis(List<Double> expUtilVMs, List<Double> expUtilVMAfter, List<Integer> removeVM){
        Double Total=0.0;
        
        Total = 0.0;
        for(int i = 0 ; i < expUtilVMAfter.size() ; i++){
            Total += expUtilVMAfter.get(i);
        }
        Double loadImbalanceFact1 = Total / expUtilVMs.size();

        Total = 0.0;
            for(int i=0; i< expUtilVMAfter.size(); i++){
                Double Cal = loadImbalanceFact1-expUtilVMAfter.get(i);
                Total += Math.pow(Cal, 2);
            }
        Total = Total / expUtilVMAfter.size();

        if(removeVM.isEmpty()){
            BigDecimal bigDecimalTotal = BigDecimal.valueOf(Total);
            BigDecimal scaled = bigDecimalTotal.setScale(1, RoundingMode.HALF_UP);

            if(scaled.compareTo(BigDecimal.ZERO)==0) {
            System.out.println("Case 1");
            return true;
        }
        }else{
            Double Threshold = (expUtilVMAfter.size()-1)*Math.pow(loadImbalanceFact1,2) / expUtilVMAfter.size();
            // System.out.println(Threshold);
            if(Total <= Threshold) {
            System.out.println("Case 2");
            return true;
            }
        }

        return false;
    }

    protected List<Cloudlet> allocateNewCloudlets(List<Double> workloadPerVM, List<Cloudlet> newCloudlets, List<Vm> vmList, List<Double> expUtilVMs){
        for (int i = 0; i < newCloudlets.size(); i++) {
            for (int j = 0; j < workloadPerVM.size(); j++) {
                Double workload = workloadPerVM.get(j);
                if (newCloudlets.get(i).getLength() < workload) {
                    workloadPerVM.set(j, workload - newCloudlets.get(i).getLength());
                    newCloudlets.get(i).setVm(vmList.get(j));
                    break;
                } 
            }
        }

        Iterator<Cloudlet> iterator = newCloudlets.iterator();
        while (iterator.hasNext()) {
            Cloudlet cloudlet = iterator.next();
            if (cloudlet.getVm() == Vm.NULL) {
                iterator.remove();
            }
        }

        for (int i = 0; i < workloadPerVM.size(); i++) {

            if(workloadPerVM.get(i) < 1.0) continue;
            int remainingWorkload = (int) workloadPerVM.get(i).doubleValue();
            Cloudlet sharedCloudlet = createCloudlet(remainingWorkload);
            sharedCloudlet.setVm(vmList.get(i));
            newCloudlets.add(sharedCloudlet);
            // createAndSubmitCloudlets(vmList.get(i), submissionDelay, 1, remainingWorkload, expUtilVMs.get(i));
        }
        
        return newCloudlets;
    }

    protected Cloudlet createCloudlet(long CLOUDLET_LENGTH) {
        //UtilizationModel defining the Cloudlets use only 50% of any resource all the time
        final var utilizationModel = new UtilizationModelDynamic(1.0);
        final var utilizationModelRam = new UtilizationModelDynamic(0.6);
        final var utilizationModelBw = new UtilizationModelDynamic(0.7);

        final var cloudlet = new CloudletSimple(CLOUDLET_LENGTH, 1, utilizationModel);
        cloudlet.setSizes(1024).setUtilizationModelRam(utilizationModelRam).setUtilizationModelBw(utilizationModelBw);
        return cloudlet;
    }
}