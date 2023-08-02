/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2021 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudsimplus.examples.TASimulation.PLAC;

import org.apache.commons.math3.linear.Array2DRowFieldMatrix;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.MarkdownTableColumn;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.examples.TASimulation.PLAC.PLACLoadBalancer;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

import org.cloudsimplus.util.Log;

/**
 * A minimal but organized, structured and re-usable CloudSim Plus example
 * which shows good coding practices for creating simulation scenarios.
 *
 * <p>It defines a set of constants that enables a developer
 * to change the number of Hosts, VMs and Cloudlets to create
 * and the number of {@link Pe}s for Hosts, VMs and Cloudlets.</p>
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 1.0
 */
public class PLAC_Demo {
    private static final int  HOSTS = 1;
    private int HOST_PES = 1;
    private int HOST_MIPS = 1000; // Milion Instructions per Second (MIPS)
    private int HOST_RAM = 512; //in Megabytes
    private long HOST_BW = 1000; //in Megabits/s
    private long HOST_STORAGE = 10_000; //in Megabytes

    private int VMS;
    private List<Integer> mips = new ArrayList<>();
    private static final int VM_PES = 1;
    private static final int CLOUDLET_PES = 1;

    private final CloudSimPlus simulation;
    private final DatacenterBroker broker0;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList_T0;
    private List<Cloudlet> cloudletList_T20;
    private List<Double> initialExecutionTimes = new ArrayList<>();
    private Datacenter datacenter0;
    private Map<Integer, Integer> map;
    private ArrayList<Integer> taskLength = new ArrayList<>();
    private final List<Integer> initialCloudlets = new ArrayList<>();
    private final int interations = 50;
    public static void main(String[] args) {
        new PLAC_Demo();
    }

    private PLAC_Demo() {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(ch.qos.logback.classic.Level.WARN);

        simulation = new CloudSimPlus();

        mips = getNumberofVMs();
        HOST_PES = HOST_PES*mips.size();
        HOST_MIPS = HOST_MIPS*mips.size();
        HOST_RAM = HOST_RAM*mips.size();
        HOST_BW = HOST_BW*mips.size();
        HOST_STORAGE = HOST_STORAGE*mips.size();

        datacenter0 = createDatacenter(HOST_PES, HOST_MIPS, HOST_RAM, HOST_BW, HOST_STORAGE);

        List<Integer> newCloudlets = getNumberOfNewCloudlets();
        //Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs and Cloudlets
        broker0 = new DatacenterBrokerSimple(simulation);

        vmList = createVms(mips);
        System.out.println(vmList);
        int initialTask = getNumberOfInitialCloudlets();
        for(int i=0; i<initialTask; i++){
            initialCloudlets.add(1000);
        }

        cloudletList_T0 = createCloudlets(initialCloudlets, initialCloudlets.size());
        calculateInitialExecutionTimes(cloudletList_T0, vmList);
        cloudletList_T20 = createCloudlets(newCloudlets, newCloudlets.size());
        int ant = vmList.size()+2;
        PLACLoadBalancer plac = new PLACLoadBalancer(ant, 1, 3, 2, 8, 0.01);
        try {
            map = plac.implement(cloudletList_T20, vmList, interations, initialExecutionTimes);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        alloacateTaskPLAC(map);

        broker0.submitVmList(vmList);
        broker0.submitCloudletList(cloudletList_T0);
        broker0.submitCloudletList(cloudletList_T20, 20);

        runSimulationAndPrintResults();
        System.out.println(getClass().getSimpleName() + " finished!");
    }

    private int getNumberOfInitialCloudlets(){
        // preparing new cloudlets
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter number of initial tasks: ");
        int initialCloudlets = scanner.nextInt();

        return initialCloudlets;
    }

    private List<Integer> getNumberofVMs(){
        List<Integer> VM_MIPS = new ArrayList<Integer>();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter Number of VMs: ");
        int VMs_Number = scanner.nextInt();

        int counter = 0;
        while (counter < VMs_Number) {
            int remaining = VMs_Number - counter;
            System.out.println("You have " + remaining + " VM left");
            System.out.println("Give VM " + counter + " computational power: ");
            int number = scanner.nextInt();
            VM_MIPS.add(number);

            counter++;
        }
        Collections.sort(VM_MIPS, Collections.reverseOrder());
        return VM_MIPS;
    }

    private void runSimulationAndPrintResults() {
        simulation.start();
        final var cloudletFinishedList = broker0.getCloudletFinishedList();
        cloudletFinishedList.sort(Comparator.comparingLong(c -> c.getVm().getId()));
        try {
        // Specify the file path to export the output
        String outputPath = "result/plac result/E1/Plac_E1_Outputtest.csv";

        // Create a FileOutputStream to write the output to the file
        FileOutputStream fileOutputStream = new FileOutputStream(outputPath);

        // Create a PrintStream to write to the file
        PrintStream printStream = new PrintStream(fileOutputStream);

        // Redirect the standard output stream to the file
        System.setOut(printStream);

        // finding makespan for each VM
        Double tmin = Double.MAX_VALUE;
        Double tmax = 0.0;
        
        List<Double> makespanVM = new ArrayList<>();
        Double makespan = 0.0;

        for(int i = 0, j = 0; i < cloudletFinishedList.size(); i++){
            Double value = cloudletFinishedList.get(i).getTotalExecutionTime();

            if(value < tmin) tmin = value;
            if(value > tmax) tmax = value;

            if(cloudletFinishedList.get(i).getVm() == vmList.get(j)){
                makespan += value;
            }else{
                makespanVM.add(makespan);
                j++;
                makespan = value;
            }
        }

        makespanVM.add(makespan);
        System.out.println("This is Makespan per vm " + makespanVM);
        
        //find total execution time
        Double total = 0.0;
        for(int i = 0; i < makespanVM.size(); i++){
            total +=makespanVM.get(i);
        }
        System.out.println("This is Total Execution Time " + total);

        Double maxMakespan = Collections.max(makespanVM); // max makespan
        System.out.println("This is Maximum Makespan " + maxMakespan);

        // Find Utilization per VM and  Average Utilization
        List<Double> utilVM = new ArrayList<>();
        Double total2 = 0.0;
        for(int i=0; i<makespanVM.size(); i++){
            Double utilVmValue = makespanVM.get(i)/maxMakespan;
            utilVM.add(utilVmValue);
            total2 += utilVmValue;
        }
        System.out.println("This is Utilization per VM " + utilVM);
        System.out.println("This is Average Utilization " + total2 / utilVM.size());

        // Find Degree of imbalance
        System.out.println("This is Degree of Imbalance " + (tmax-tmin)/(total/cloudletFinishedList.size()));

        new CloudletsTableBuilder(cloudletFinishedList)
        .addColumn(new MarkdownTableColumn(" Vm ", "ID"), c -> c.getVm())
        .build();
        // Close the output stream
        printStream.close();

        System.out.println("Terminal output exported to " + outputPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a Datacenter and its Hosts.
     * @param hOST_STORAGE2
     * @param hOST_BW2
     * @param hOST_RAM2
     * @param hOST_MIPS2
     * @param hOST_PES2
     */
    private Datacenter createDatacenter(int HOST_PES, int HOST_MIPS, int HOST_RAM, long HOST_BW, long HOST_STORAGE) {
        final var hostList = new ArrayList<Host>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            final var host = createHost(HOST_PES, HOST_MIPS, HOST_RAM, HOST_BW, HOST_STORAGE);
            hostList.add(host);
        }

        //Uses a VmAllocationPolicySimple by default to allocate VMs
        return new DatacenterSimple(simulation, hostList);
    }

    private Host createHost(int HOST_PES, int HOST_MIPS, int HOST_RAM, long HOST_BW, long HOST_STORAGE) {
        final var peList = new ArrayList<Pe>(HOST_PES);
        //List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            //Uses a PeProvisionerSimple by default to provision PEs for VMs
            peList.add(new PeSimple(HOST_MIPS));
        }

        /*
        Uses ResourceProvisionerSimple by default for RAM and BW provisioning
        and VmSchedulerSpaceShared for VM scheduling.
        */
        return new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList);
    }

    /**
     * Creates a list of VMs.
     */
    private List<Vm> createVms(List<Integer> mips) {
        final var vmList = new ArrayList<Vm>(VMS);
        for (int i = 0; i < mips.size(); i++) {
            //Uses a CloudletSchedulerTimeShared by default to schedule Cloudlets
            final var vm = new VmSimple(mips.get(i), VM_PES);
            vm.setRam(512).setBw(1000).setSize(10_000).setCloudletScheduler(new CloudletSchedulerSpaceShared());
            vmList.add(vm);
        }

        return vmList;
    }

    /**
     * Creates a list of Cloudlets.
     */
    private List<Cloudlet> createCloudlets(List<Integer> CLOUDLET_LENGTH, int CLOUDLETS_TOTAL) {
        final var cloudletList = new ArrayList<Cloudlet>(CLOUDLETS_TOTAL);

        //UtilizationModel defining the Cloudlets use only 50% of any resource all the time
        final var utilizationModel = new UtilizationModelDynamic(1.0);
        final var utilizationModelRam = new UtilizationModelDynamic(0.6);
        final var utilizationModelBw = new UtilizationModelDynamic(0.7);

        for (int i = 0; i < CLOUDLETS_TOTAL; i++) {
            final var cloudlet = new CloudletSimple(CLOUDLET_LENGTH.get(i), CLOUDLET_PES, utilizationModel);
            cloudlet.setSizes(300).setUtilizationModelRam(utilizationModelRam).setUtilizationModelBw(utilizationModelBw);
            cloudletList.add(cloudlet);
        }

        return cloudletList;
    }

    private void calculateInitialExecutionTimes(List<Cloudlet> cloudletList_T0, List<Vm> vmList){
    int j = 0;
    boolean resetJ = false; // Initialize the reset flag
    for (int i = 0; i < cloudletList_T0.size(); i++) {
        Double total = cloudletList_T0.get(i).getLength() / vmList.get(j).getMips();
        cloudletList_T0.get(i).setVm(vmList.get(j));
        if (resetJ) {
            total += initialExecutionTimes.get(j);
            initialExecutionTimes.set(j, total);
            resetJ = false; // Reset the flag after processing the else-if block
        } else {
            initialExecutionTimes.add(total); 
        }

        j++;
        
        if (j == vmList.size()) {
            j = 0;
            resetJ = true; // Set the reset flag when j is reset to 0
        }
    }
}


    private List<Integer> getNumberOfNewCloudlets(){
        List<Integer> newTasks = new ArrayList<Integer>();

        // preparing new cloudlets
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter total of task desired in next timeslot: ");
        int newCloudlets = scanner.nextInt();

        for (int i = 0; i < newCloudlets; i++) {
            System.out.print("Enter Task #" + (i + 1) + " size : ");
            int input = scanner.nextInt();
            newTasks.add(input);
        }

        Collections.sort(newTasks, Collections.reverseOrder());
        return newTasks;
    }

    private void alloacateTaskPLAC(Map<Integer, Integer> map){
        for (int i = 0; i < cloudletList_T20.size(); i++) {
            if (map.containsKey(i)) {
                int value = map.get(i);
                // System.out.println("Key: " + i + ", Value: " + value);
                cloudletList_T20.get(i).setVm(vmList.get(value));
            } else {
                System.out.println("No value found for key: " + i);
            }
        }
    }
}
