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
package org.cloudsimplus.examples.schedulers;

import org.cloudsimplus.allocationpolicies.VmAllocationPolicyFirstFit;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.util.ArrayList;
import java.util.List;

/**
 * An example showing the usage of the {@link VmSchedulerTimeShared}
 * which allows multiple VMs to use the same PEs from a Host,
 * provided those PEs have available MIPS capacity.
 *
 * <p>In this example we have 2 Hosts with 2 PEs each one.
 * Each PE having a capacity of 1000 MIPS (2000 MIPS by Host),
 * <b>totaling 4 PEs for those 2 Hosts (4000 MIPS at total)</b>.
 * Then we create 8 VMs, each one requiring a single PE.
 * <b>That would require a total of 8 PEs from existing PMs</b>,
 * which there aren't available.
 * </p>
 *
 * <p>Each VM requires just 500 MIPS and the MIPS required by all 8 VMs
 *  totals 4000 MIPS, which is the total capacity of the existing Hosts.
 *  Since we are using a {@link VmSchedulerTimeShared}, such an allocation is possible
 *  by sharing Hosts's PEs between running VMs. Each VM will use half of the MIPS capacity
 *  of the Host's PEs. Since the entire MIPS capacity required by each VM will
 *  be allocated to them, there is no performance degradation.
 *  Every Cloudlet will finish in 10 seconds, as if they weren't sharing any PE.</p>
 *
 *  <p>Using a {@link VmSchedulerSpaceShared}, the allocation of some
 *  VMs will just fail due to lack of PEs.
 *  For the current scenario with {@link VmSchedulerTimeShared}, some VMs will fail due to lack
 *  of MIPS capacity only if we increase the number of VMs.</p>
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 5.1.4
 */
public class VmSchedulerTimeSharedExample {
    private static final int HOSTS     = 2;
    private static final int VMS       = 8;

    private static final long VM_RAM  = 1000;
    private static final long VM_BW   = 1000;
    private static final long VM_SIZE = 1000;

    private static final long HOST_RAM = VM_RAM * VMS; //in Megabytes
    private static final long HOST_BW = VM_BW * VMS; //in Megabits/s
    private static final long HOST_STORAGE = VM_SIZE * VMS; //in Megabytes

    private static final int HOST_PES     = 2;
    private static final int VM_PES       = 1;
    private static final int CLOUDLET_PES = VM_PES;

    private static final int HOST_MIPS = 1000;
    private static final int VM_MIPS   = HOST_MIPS/2;

    private static final int CLOUDLET_LENGTH = HOST_MIPS*10;

    private final CloudSimPlus simulation;
    private final DatacenterBroker broker0;
    private List<Host> hostList;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList;
    private static final int SCHEDULING_INTERVAL = 10;

    public static void main(String[] args) {
        new VmSchedulerTimeSharedExample();
    }

    private VmSchedulerTimeSharedExample() {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        //Log.setLevel(ch.qos.logback.classic.Level.WARN);

        simulation = new CloudSimPlus();
        hostList = new ArrayList<>(HOSTS);
        vmList = new ArrayList<>(VMS);
        cloudletList = new ArrayList<>(VMS);

        createDatacenter();

        //Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs and Cloudlets
        broker0 = new DatacenterBrokerSimple(simulation);

        createVms();
        createCloudlets();
        broker0.submitVmList(vmList);
        broker0.submitCloudletList(cloudletList);

        simulation.start();

        new CloudletsTableBuilder(broker0.getCloudletFinishedList())
            .addColumn(new TextTableColumn("Host MIPS", "total"), c -> c.getVm().getHost().getTotalMipsCapacity(), 5)
            .addColumn(new TextTableColumn("VM MIPS", "total"), c -> c.getVm().getTotalMipsCapacity(), 8)
            .addColumn(new TextTableColumn("  VM MIPS", "requested"), this::getVmRequestedMips, 9)
            .addColumn(new TextTableColumn("  VM MIPS", "allocated"), this::getVmAllocatedMips, 10)
            .build();

        System.out.printf("%n-------------------------------------- Hosts CPU usage History --------------------------------------%n");
        hostList.forEach(this::printHostStateHistory);

        for (final var vm : vmList) {
            System.out.printf("%s: Requested MIPS: %.0f Allocated MIPS: %.0f%n", vm, vm.getCurrentRequestedMips(), vm.getMips());
        }
    }

    /**
     * Prints the {@link Host#getStateHistory() state history} for a given Host.
     * Realize that the state history is just collected if that is enabled before
     * starting the simulation by calling {@link Host#setStateHistoryEnabled(boolean)}.
     *
     * @param host
     */
    private void printHostStateHistory(Host host) {
        System.out.printf("Host: %d%n", host.getId());
        System.out.println("-----------------------------------------------------------------------------------------------------");
        host.getStateHistory().stream().forEach(System.out::print);
        System.out.println();
    }

    private double getVmRequestedMips(Cloudlet c) {
        if(c.getVm().getStateHistory().isEmpty()){
            return 0;
        }

        return c.getVm().getStateHistory().get(c.getVm().getStateHistory().size()-1).getRequestedMips();
    }

    private double getVmAllocatedMips(Cloudlet c) {
        if(c.getVm().getStateHistory().isEmpty()){
            return 0;
        }

        return c.getVm().getStateHistory().get(c.getVm().getStateHistory().size()-1).getAllocatedMips();
    }

    /**
     * Creates a Datacenter and its Hosts.
     */
    private void createDatacenter() {
        for(int h = 0; h < HOSTS; h++) {
            final var host = createHost();
            hostList.add(host);
        }

        Datacenter dc0 = new DatacenterSimple(simulation, hostList, new VmAllocationPolicyFirstFit());
        dc0.setSchedulingInterval(SCHEDULING_INTERVAL);
    }

    private Host createHost() {
        final var peList = new ArrayList<Pe>(HOST_PES);
        //List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(HOST_MIPS));
        }

        final var host = new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList);
        host.setStateHistoryEnabled(true)
            .setVmScheduler(new VmSchedulerTimeShared());
        return host;
    }

    /**
     * Creates a list of VMs for the {@link #broker0}.
     */
    private void createVms() {
        for (int i = 0; i < VMS; i++) {
            final var vm =
                new VmSimple(VM_MIPS, VM_PES)
                    .setRam(VM_RAM).setBw(VM_BW).setSize(VM_SIZE);
            vmList.add(vm);
        }
    }

    /**
     * Creates one Cloudlet for each VM belonging to {@link #broker0}.
     */
    private void createCloudlets() {
        for (final var vm : vmList) {
            final var cloudlet = new CloudletSimple(CLOUDLET_LENGTH, CLOUDLET_PES);
            cloudlet.setVm(vm);
            cloudletList.add(cloudlet);
        }
    }
}

