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
package org.cloudsimplus.examples.TASimulation.SA;

import ch.qos.logback.classic.Level;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerHeuristic;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.MarkdownTableColumn;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.distributions.UniformDistr;
import org.cloudsimplus.heuristics.CloudletToVmMappingHeuristic;
import org.cloudsimplus.heuristics.CloudletToVmMappingSimulatedAnnealing;
import org.cloudsimplus.heuristics.CloudletToVmMappingSolution;
import org.cloudsimplus.heuristics.HeuristicSolution;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.provisioners.ResourceProvisionerSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.util.Log;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.utilizationmodels.UtilizationModelFull;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * An example that uses a
 * <a href="http://en.wikipedia.org/wiki/Simulated_annealing">Simulated Annealing</a>
 * heuristic to find a suboptimal mapping between Cloudlets and Vm's submitted to a
 * DatacenterBroker. The number of {@link Pe}s of Vm's and Cloudlets are defined
 * randomly.
 *
 * <p>The {@link DatacenterBrokerHeuristic} is used
 * with the {@link CloudletToVmMappingSimulatedAnnealing} class
 * in order to find an acceptable solution with a high
 * {@link HeuristicSolution#getFitness() fitness value}.</p>
 *
 * <p>Different {@link CloudletToVmMappingHeuristic} implementations can be used
 * with the {@link DatacenterBrokerHeuristic} class.</p>
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 1.0
 */
public class SA_Demo {
    private final DatacenterBroker broker0;
    private Datacenter datacenter0;

    private static final int HOSTS_TO_CREATE = 1;
    private static int VMS_TO_CREATE;

    /**
     * Simulated Annealing (SA) parameters.
     */
    public static final double SA_INITIAL_TEMPERATURE = 1.0;
    public static final double SA_COLD_TEMPERATURE = 0.0001;
    public static final double SA_COOLING_RATE = 0.003;
    public static final int    SA_NUMBER_OF_NEIGHBORHOOD_SEARCHES = 50;

    private final CloudSimPlus simulation;
    private final List<Cloudlet> cloudletList;
    private List<Vm> vmList;
    private List<Integer> mips = new ArrayList<>();
    private final ArrayList<Integer> taskLength = new ArrayList<>();
    private Integer initialCloudletsTotal;
    private CloudletToVmMappingSimulatedAnnealing heuristic;

    /**
     * Number of cloudlets created so far.
     */
    private int createdCloudlets = 0;
    /**
     * Number of VMs created so far.
     */
    private int createdVms = 0;
    /**
     * Number of hosts created so far.
     */
    private int createdHosts = 0;

    public static void main(String[] args) {
        new SA_Demo();
    }

    /**
     * Default constructor where the simulation is built.
     */
    private SA_Demo() {
        //Enables just some level of log messages.
        Log.setLevel(Level.WARN);

        System.out.println("Starting " + getClass().getSimpleName());
        this.vmList = new ArrayList<>();
        this.cloudletList = new ArrayList<>();

        simulation = new CloudSimPlus();

        mips = getNumberofVMs();
        int HOST_PES = 1*mips.size();
        int HOST_MIPS = 1000*mips.size();
        int HOST_RAM = 512*mips.size();
        long HOST_BW = 1000*mips.size();
        long HOST_STORAGE = 10_000*mips.size();

        datacenter0 = createDatacenter(HOST_PES, HOST_MIPS, HOST_RAM, HOST_BW, HOST_STORAGE);
        broker0 = createDatacenterBrokerHeuristic();

        List<Integer> newCloudlets = getNumberOfNewCloudlets();

        createAndSubmitVms(mips);
        System.out.println(vmList);

        initialCloudletsTotal = getNumberOfInitialCloudlets();
        createAndSubmitInitialCloudlet(initialCloudletsTotal, vmList);
        createAndSubmitCloudlets(newCloudlets, newCloudlets.size());

        runSimulationAndPrintResults();
    }

    private Integer getNumberOfInitialCloudlets() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter number of initial tasks: ");
        int initialCloudlets = scanner.nextInt();

        return initialCloudlets;
    }

    private List<Integer> getNumberofVMs() {
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
        
        return VM_MIPS;
    }

    private void runSimulationAndPrintResults() {
        simulation.start();
        final var cloudletFinishedList = broker0.getCloudletFinishedList();
        cloudletFinishedList.sort(Comparator.comparingLong(c -> c.getVm().getId()));
        try {
        // Specify the file path to export the output
        String outputPath = "result/SA result/E1/SA_E1_Outputtest.csv";

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

	private DatacenterBrokerHeuristic createDatacenterBrokerHeuristic() {
		createSimulatedAnnealingHeuristic();
		final var broker0 = new DatacenterBrokerHeuristic(simulation);
		broker0.setHeuristic(heuristic);
		return broker0;
	}

	private void createAndSubmitCloudlets(List<Integer> CLOUDLETS_LENGTH, int CLOUDLETS_TOTAL) {
        final var cloudletList = new ArrayList<Cloudlet>(CLOUDLETS_TOTAL);

		for(int i = 0; i < CLOUDLETS_TOTAL; i++){
		    cloudletList.add(createCloudlet(CLOUDLETS_LENGTH.get(i), broker0, 1));
		}
		broker0.submitCloudletList(cloudletList, 20);
	}

    private void createAndSubmitInitialCloudlet(Integer initialCloudletsTotal, List<Vm> vmList) {
        boolean resetJ = false; // Initialize the reset flag
		for (int i = 0, j = 0; i < initialCloudletsTotal; i++) {
            System.out.println(j);
            cloudletList.add(createInitialCloudlets(broker0, 1, vmList.get(j)));
            if (resetJ) {
                resetJ = false;
            }
            j++;
            if (j == vmList.size()) { // Change the condition here
                j = 0;
                resetJ = true; // Set the reset flag when j is reset to 0
            }
        }
        
		broker0.submitCloudletList(cloudletList);
	}

	private void createAndSubmitVms(List<Integer> mips) {
		vmList = new ArrayList<>(mips.size());
		for(int i = 0; i < mips.size(); i++){
		    vmList.add(createVm(mips.get(i), broker0, 1));
		}
		broker0.submitVmList(vmList);
	}

	private void createSimulatedAnnealingHeuristic() {
		heuristic = new CloudletToVmMappingSimulatedAnnealing(SA_INITIAL_TEMPERATURE, new UniformDistr(0, 1));
        heuristic.setColdTemperature(SA_COLD_TEMPERATURE)
                 .setCoolingRate(SA_COOLING_RATE)
                 .setSearchesByIteration(SA_NUMBER_OF_NEIGHBORHOOD_SEARCHES);
	}

	// private void print(final DatacenterBrokerHeuristic broker0) {
    //     final double roundRobinMappingCost = computeRoundRobinMappingCost();
	// 	printSolution(
	// 	        "Heuristic solution for mapping cloudlets to Vm's         ",
	// 	        heuristic.getBestSolutionSoFar(), false);

	// 	System.out.printf(
	// 	    "\tThe heuristic solution cost represents %.2f%% of the round robin mapping cost used by the DatacenterBrokerSimple%n",
	// 	    heuristic.getBestSolutionSoFar().getCost()*100.0/roundRobinMappingCost);
	// 	System.out.printf("\tThe solution finding spend %.2f seconds to finish%n", broker0.getHeuristic().getSolveTime());
	// 	System.out.println("\tSimulated Annealing Parameters");
    //     System.out.printf("\t\tNeighborhood searches by iteration: %d%n", SA_NUMBER_OF_NEIGHBORHOOD_SEARCHES);
    //     System.out.printf("\t\tInitial Temperature: %18.6f%n", SA_INITIAL_TEMPERATURE);
    //     System.out.printf("\t\tCooling Rate       : %18.6f%n", SA_COOLING_RATE);
    //     System.out.printf("\t\tCold Temperature   : %18.6f%n%n", SA_COLD_TEMPERATURE);
    //     System.out.println(getClass().getSimpleName() + " finished!");
	// }

	/**
     * Randomly gets a number of PEs (CPU cores).
     *
     * @param maxPesNumber the maximum value to get a random number of PEs
     * @return the randomly generated PEs number
     */
    // private int getRandomPesNumber(final int maxPesNumber) {
    //     return heuristic.getRandomValue(maxPesNumber)+1;
    // }

    private DatacenterSimple createDatacenter(int HOST_PES, int HOST_MIPS, int HOST_RAM, long HOST_BW, long HOST_STORAGE) {
        final var hostList = new ArrayList<Host>();
        for(int i = 0; i < HOSTS_TO_CREATE; i++) {
            hostList.add(createHost(HOST_PES, HOST_MIPS, HOST_RAM, HOST_BW, HOST_STORAGE));
        }

        return new DatacenterSimple(simulation, hostList);
    }

    private Host createHost(int HOST_PES, int HOST_MIPS, int HOST_RAM, long HOST_BW, long HOST_STORAGE) {

        final var peList = new ArrayList<Pe>();
        /*Creates the Host's CPU cores and defines the provisioner
        used to allocate each core for requesting VMs.*/
        for(int i = 0; i < HOST_PES; i++)
            peList.add(new PeSimple(HOST_MIPS));

       return new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList)
                   .setRamProvisioner(new ResourceProvisionerSimple())
                   .setBwProvisioner(new ResourceProvisionerSimple());
    }

    private Vm createVm(int mips, final DatacenterBroker broker, final int pesNumber) {
        final long   storage = 10_000; // vm image size (Megabyte)
        final int    ram = 512; // vm memory (Megabyte)
        final long   bw = 1000; // vm bandwidth

        return new VmSimple(createdVms++, mips, pesNumber)
            .setRam(ram).setBw(bw).setSize(storage)
            .setCloudletScheduler(new CloudletSchedulerSpaceShared());
    }

    private Cloudlet createCloudlet(Integer length, final DatacenterBroker broker, final int pesNumber) {
        final long fileSize = 300; //Size (in bytes) before execution
        final long outputSize = 300; //Size (in bytes) after execution

        final var utilizationFull = new UtilizationModelDynamic(1.0);
        final var utilizationModelRam = new UtilizationModelDynamic(0.6);
        final var utilizationModelBw = new UtilizationModelDynamic(0.7);

        return new CloudletSimple(createdCloudlets++, length, pesNumber)
                    .setFileSize(fileSize)
                    .setOutputSize(outputSize)
                    .setUtilizationModelCpu(utilizationFull)
                    .setUtilizationModelRam(utilizationModelRam)
                    .setUtilizationModelBw(utilizationModelBw);
    }

    private Cloudlet createInitialCloudlets(final DatacenterBroker broker, final int pesNumber, Vm vm) {
        final long length = 1000; //in Million Instructions (MI)
        final long fileSize = 300; //Size (in bytes) before execution
        final long outputSize = 300; //Size (in bytes) after execution

        final var utilizationFull = new UtilizationModelDynamic(1.0);
        final var utilizationModelRam = new UtilizationModelDynamic(0.6);
        final var utilizationModelBw = new UtilizationModelDynamic(0.7);

        return new CloudletSimple(createdCloudlets++, length, pesNumber)
                    .setFileSize(fileSize)
                    .setOutputSize(outputSize)
                    .setUtilizationModelCpu(utilizationFull)
                    .setUtilizationModelRam(utilizationModelRam)
                    .setUtilizationModelBw(utilizationModelBw)
                    .setVm(vm);
    }

    // private double computeRoundRobinMappingCost() {
    //     final var roundRobinSolution = new CloudletToVmMappingSolution(heuristic);
    //     int i = 0;
    //     for (Cloudlet c : cloudletList) {
    //         //cyclically selects a Vm (as in a circular queue)
    //         roundRobinSolution.bindCloudletToVm(c, vmList.get(i));
    //         i = (i+1) % vmList.size();
    //     }

    //     printSolution(
    //         "Round robin solution used by DatacenterBrokerSimple class",
    //         roundRobinSolution, false);
    //     return roundRobinSolution.getCost();
    // }

    // private void printSolution(
    //     final String title,
    //     final CloudletToVmMappingSolution solution,
    //     final boolean showIndividualCloudletFitness)
    // {
    //     System.out.printf("%n%s (cost %.2f fitness %.6f)%n",
    //             title, solution.getCost(), solution.getFitness());
    //     if(!showIndividualCloudletFitness)
    //         return;

    //     for(Map.Entry<Cloudlet, Vm> e: solution.getResult().entrySet()){
    //         System.out.printf(
    //             "Cloudlet %3d (%d PEs, %6d MI) mapped to Vm %3d (%d PEs, %6.0f MIPS)%n",
    //             e.getKey().getId(),
    //             e.getKey().getPesNumber(), e.getKey().getLength(),
    //             e.getValue().getId(),
    //             e.getValue().getPesNumber(), e.getValue().getMips());
    //     }

    //     System.out.println();
    // }

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

        return newTasks;
    }
}