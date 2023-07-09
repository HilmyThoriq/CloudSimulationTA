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
import org.cloudsimplus.examples.TASimulation.PLAC.PLACLoadBalancer;
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
public class PLAC_E2 {
    private static final int  HOSTS = 1;
    private static final int  HOST_PES = 20;
    private static final int  HOST_MIPS = 1000*20; // Milion Instructions per Second (MIPS)
    private static final int  HOST_RAM = 512*20; //in Megabytes
    private static final long HOST_BW = 1000*20; //in Megabits/s
    private static final long HOST_STORAGE = 10_000*20; //in Megabytes

    private static final int VMS = 20;
    private final List<Integer> mips = new ArrayList<>(Arrays.asList(962, 933, 875, 847, 803, 789, 725, 615, 607, 568, 447, 436, 341, 305, 248, 203, 196, 176, 157, 155));
    private static final int VM_PES = 1;

    private static final int CLOUDLETS = 10;
    private static final int CLOUDLET_PES = 1;
    private static final int CLOUDLET_LENGTH = 1000; // Milion Instructions (MI)

    private final CloudSimPlus simulation;
    private final DatacenterBroker broker0;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList_T0;
    private List<Cloudlet> cloudletList_T20;
    private List<Double> initialExecutionTimes = new ArrayList<>();
    private Datacenter datacenter0;
    private Map<Integer, Integer> map;
    private final ArrayList<Integer> taskLength = new ArrayList<>(Arrays.asList(9449, 8772, 4951, 4574, 2858, 2741));
    private final ArrayList<Integer> initialCloudlets = new ArrayList<>();
    private final int interations = 50;
    public static void main(String[] args) {
        new PLAC_E2();
    }

    private PLAC_E2() {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(ch.qos.logback.classic.Level.WARN);

        simulation = new CloudSimPlus();
        datacenter0 = createDatacenter();

        List<Integer> newCloudlets = getNumberOfNewCloudlets();
        //Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs and Cloudlets
        broker0 = new DatacenterBrokerSimple(simulation);

        vmList = createVms(mips);

        for(int i=0; i<100; i++){
            initialCloudlets.add(1000);
        }

        cloudletList_T0 = createCloudlets(initialCloudlets, initialCloudlets.size());

        calculateInitialExecutionTimes();
        // System.out.println(initialExecutionTimes);
        cloudletList_T20 = createCloudlets(newCloudlets, newCloudlets.size());

        PLACLoadBalancer plac = new PLACLoadBalancer(100, 1, 3, 2, 8, 0.01);
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

     private void runSimulationAndPrintResults() {
        simulation.start();
        final var cloudletFinishedList = broker0.getCloudletFinishedList();
        cloudletFinishedList.sort(Comparator.comparingLong(c -> c.getVm().getId()));
        try {
        // Specify the file path to export the output
        String outputPath = "result/plac result/E2/plac_E2_Outputtest.csv";

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
        System.out.println("This is Tmax " + tmax);
        System.out.println("This is Tmin " + tmin);
        System.out.println("This is Tavg " + total/cloudletFinishedList.size()); 
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
     */
    private Datacenter createDatacenter() {
        final var hostList = new ArrayList<Host>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            final var host = createHost();
            hostList.add(host);
        }

        //Uses a VmAllocationPolicySimple by default to allocate VMs
        return new DatacenterSimple(simulation, hostList);
    }

    private Host createHost() {
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
        for (int i = 0, j = 0; i < VMS; i++) {
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

    private void calculateInitialExecutionTimes(){
        int initialTaskEachVM = cloudletList_T0.size()/vmList.size();
        int j = 0;
        Double total = 0.0;
        for (int i = 0; i < cloudletList_T0.size(); i++) {
            total += cloudletList_T0.get(i).getLength()/vmList.get(j).getMips();
            cloudletList_T0.get(i).setVm(vmList.get(j));

            // Move to the next index of A when it reaches multiple of each
            if ((i + 1) % initialTaskEachVM == 0) {
                initialExecutionTimes.add(total);
                total = 0.0;
                j++;
            }
        }
    }

    private List<Integer> getNumberOfNewCloudlets(){
        List<Integer> newTasks = new ArrayList<Integer>();

        // preparing new cloudlets
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter total of task desired in next timeslot: ");
        int newCloudlets = scanner.nextInt();
        List<Integer> allocations = new ArrayList<Integer>(taskLength.size());
       
        int sum = 0;
        int counter = 0;
        int taskCounter = 0;
        while (counter < taskLength.size()) {
            System.out.println("How many " + taskLength.get(taskCounter) + " you want to have?");
            int number = scanner.nextInt();
            allocations.add(number);

            sum += number;

            int remaining = newCloudlets - sum;
            System.out.println("Remaining: " + remaining + "\n");

            if (sum > newCloudlets) {
                sum = 0;
                counter = 0;
                remaining = 0;
                System.out.println("Total sum exceeds " + newCloudlets + ". Restarting from the beginning.");
                taskCounter=0;
                allocations.clear();
                continue;
            }
            taskCounter++;
            counter++;
        }

        for (int i = 0; i < taskLength.size(); i++) {
            // for each task value, iterate num.get(i) times and add the task value to the result list
            for (int j = 0; j < allocations.get(i); j++) {
                newTasks.add(taskLength.get(i));
            }
        }

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
