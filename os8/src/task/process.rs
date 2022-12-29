use super::id::RecycleAllocator;
use super::{add_task, pid_alloc, PidHandle, TaskControlBlock};
use crate::fs::{File, Stdin, Stdout};
use crate::mm::{translated_refmut, MemorySet, KERNEL_SPACE};
use crate::sync::{Condvar, Mutex, Semaphore, UPSafeCell};
use crate::trap::{trap_handler, TrapContext};
use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefMut;

pub struct ProcessControlBlock {
    // immutable
    pub pid: PidHandle,
    // mutable
    inner: UPSafeCell<ProcessControlBlockInner>,
}

// LAB5 HINT: you may add data structures for deadlock detection here
pub struct ProcessControlBlockInner {
    pub is_zombie: bool,
    pub memory_set: MemorySet,
    pub parent: Option<Weak<ProcessControlBlock>>,
    pub children: Vec<Arc<ProcessControlBlock>>,
    pub exit_code: i32,
    pub fd_table: Vec<Option<Arc<dyn File + Send + Sync>>>,
    pub tasks: Vec<Option<Arc<TaskControlBlock>>>,
    pub task_res_allocator: RecycleAllocator,
    pub mutex_list: Vec<Option<Arc<dyn Mutex>>>,
    pub semaphore_list: Vec<Option<Arc<Semaphore>>>,
    pub condvar_list: Vec<Option<Arc<Condvar>>>,
    pub detector : Option<Arc<DeadLockDetector>>,
}

pub struct DeadLockDetector{
    pub inner : UPSafeCell<DeadLockDetectorInner>,
}

pub struct DeadLockDetectorInner{
    pub mutex_allocation : Vec<Option<Vec<i32>>>,
    pub mutex_need : Vec<Option<Vec<i32>>>,
    pub mutex_work : Vec<i32>,
    pub mutex_finished : Vec<bool>,

    pub semaphore_need : Vec<Option<Vec<i32>>>,
    pub semaphore_allocation : Vec<Option<Vec<i32>>>,
    pub semaphore_work : Vec<i32>,
    pub semaphore_finished : Vec<bool>,
}

impl DeadLockDetector{
    pub fn new() -> Self{
        let mut mutex_allocation :Vec<Option<Vec<i32>>> = Vec::new();
        mutex_allocation.push(Some(Vec::new())); 
        let mutex_work:Vec<i32> = Vec::new();
        let mut mutex_need:Vec<Option<Vec<i32>>> = Vec::new();
        mutex_need.push(Some(Vec::new()));
        let mutex_finished : Vec<bool> = vec![false];

        let mut semaphore_allocation : Vec<Option<Vec<i32>>> = Vec::new();
        semaphore_allocation.push(Some(Vec::new()));
        let semaphore_work : Vec<i32> = Vec::new();
        let mut semaphore_need : Vec<Option<Vec<i32>>> = Vec::new();
        semaphore_need.push(Some(Vec::new()));
        let semaphore_finished : Vec<bool> = vec![false];


        unsafe{
            DeadLockDetector{
                inner: UPSafeCell::new(DeadLockDetectorInner{
                    mutex_allocation,
                    mutex_need,
                    mutex_work,
                    mutex_finished,

                    semaphore_allocation,
                    semaphore_need,
                    semaphore_work,
                    semaphore_finished,
                })
            }
        }
    }

    pub fn add_new_mutex(&self){
        let mut inner = self.inner.exclusive_access();
        for o in inner.mutex_allocation.iter_mut(){
            if let Some(each_thread_allocation) = o{
                each_thread_allocation.push(0);
            }
        }
        for option_each_thread_need in inner.mutex_need.iter_mut(){
            if let Some(each_thread_need) = option_each_thread_need{
                each_thread_need.push(0);
            }
        }
        inner.mutex_work.push(1);
    }
    
    pub fn add_new_semaphore(&self,size:usize){
        let mut inner = self.inner.exclusive_access();
        for option_each_thread_allocation in inner.semaphore_allocation.iter_mut(){
            if let Some(each_thread_allocation) = option_each_thread_allocation{
                each_thread_allocation.push(0);
            }
        }
        for option_each_thread_need in inner.semaphore_need.iter_mut(){
            if let Some(each_thread_need) = option_each_thread_need{
                each_thread_need.push(0);
            }
        }
        inner.semaphore_work.push(size as i32);
    }

    pub fn add_new_threads(&self,thread_id:usize){
        let mut inner = self.inner.exclusive_access();
        while thread_id >= inner.mutex_allocation.len(){
            inner.mutex_allocation.push(None);
            inner.mutex_need.push(None);
            inner.mutex_finished.push(true);

            inner.semaphore_allocation.push(None);
            inner.semaphore_need.push(None);
            inner.semaphore_finished.push(true);
        }
        // there is at least one main thread at the beginning
        inner.mutex_allocation[thread_id] = Some(vec![0;inner.mutex_work.len()]);
        inner.mutex_need[thread_id] = Some(vec![0;inner.mutex_work.len()]);
        inner.mutex_finished[thread_id] = false;

        inner.semaphore_allocation[thread_id] = Some(vec![0;inner.semaphore_work.len()]);
        inner.semaphore_need[thread_id] = Some(vec![0;inner.semaphore_work.len()]);
        inner.semaphore_finished[thread_id] = false;
    }
    pub fn mutex_alloc(&self,thread_id:usize,mutex_id:usize) -> bool{
        let mut inner = self.inner.exclusive_access();
        // 修改need矩阵
        let need = inner.mutex_need[thread_id].as_mut().unwrap();
        need[mutex_id] += 1;
        drop(need);

        for i in 0..inner.mutex_finished.len(){
            let is_finished = inner.mutex_finished[i];
            if is_finished == false{
                let need = inner.mutex_need[i].as_ref().unwrap();
                if need.iter().zip(inner.mutex_work.iter()).all(|(a,b)|*a <= *b){
                    // 满足条件的线程

                    // 清空need,删除work的资源，增加allocation的资源
                    let mut allocation = inner.mutex_allocation[i].take().unwrap();
                    let mut need = inner.mutex_need[i].take().unwrap();
                    let work = &mut inner.mutex_work;
                    need.iter_mut().zip(work.iter_mut()).zip(allocation.iter_mut()).for_each(|((need,work),allocation)|{
                        *allocation += *need;
                        *work -= *need;
                        *need = 0;
                    });
                    inner.mutex_allocation[i] = Some(allocation);
                    inner.mutex_need[i] = Some(need);
                    return true;
                }
            }
        }
        false
    }

    pub fn mutex_dealloc(&self,thread_id:usize){
        let mut inner = self.inner.exclusive_access();
        inner.mutex_need[thread_id] = None;
        inner.mutex_finished[thread_id] = true;
        let mut allocation = inner.mutex_allocation[thread_id].take().unwrap();
        for (alloationc,work) in allocation.iter_mut().zip(inner.mutex_work.iter_mut()){
            *work += *alloationc;
        }
    }

    pub fn sem_alloc(&self,thread_id:usize,sem_id:usize) -> bool{
        println!("thread {} sem {}",thread_id,sem_id);
        let mut inner = self.inner.exclusive_access();
        // 修改need矩阵
        let need = inner.semaphore_need[thread_id].as_mut().unwrap();
        need[sem_id] += 1;
        drop(need);
        // // debug
        // println!("before:");
        // println!("allocation:");
        // for i in inner.semaphore_allocation.iter(){
        //     if let Some(i) = i{
        //         for j in i.iter(){
        //             print!("{} ",j);
        //         }
        //     }
        //     println!("");
        // }
        // println!("need:");
        // for i in inner.semaphore_need.iter(){
        //     if let Some(i) = i{
        //         for j in i.iter(){
        //             print!("{} ",j);
        //         }
        //     }
        //     println!("");
        // }
        // println!("work:");
        // for i in inner.semaphore_work.iter(){
        //     print!("{} ",i);
        // }
        // println!("");
        // println!("--------------------");
        let mut is_safe = false;
        for i in 0..inner.semaphore_finished.len(){
            let is_finished = inner.semaphore_finished[i];
            if is_finished == false{
                let need = inner.semaphore_need[i].as_ref().unwrap();
                if need.iter().zip(inner.semaphore_work.iter()).all(|(a,b)|*a <= *b){
                    // 满足条件的线程
                    // 清空need,删除work的资源，增加allocation的资源
                    let mut allocation = inner.semaphore_allocation[i].take().unwrap();
                    let mut need = inner.semaphore_need[i].take().unwrap();
                    let work = &mut inner.semaphore_work;
                    need.iter_mut().zip(work.iter_mut()).zip(allocation.iter_mut()).for_each(|((need,work),allocation)|{
                        *allocation += *need;
                        *work -= *need;
                        *need = 0;
                    });
                    inner.semaphore_allocation[i] = Some(allocation);
                    inner.semaphore_need[i] = Some(need);
                    is_safe = true;
                }
            }
        }
        // println!("after:");
        // println!("allocation:");
        // for i in inner.semaphore_allocation.iter(){
        //     if let Some(i) = i{
        //         for j in i.iter(){
        //             print!("{} ",j);
        //         }
        //     }
        //     println!("");
        // }
        // println!("need:");
        // for i in inner.semaphore_need.iter(){
        //     if let Some(i) = i{
        //         for j in i.iter(){
        //             print!("{} ",j);
        //         }
        //     }
        //     println!("");
        // }
        // println!("work:");
        // for i in inner.semaphore_work.iter(){
        //     print!("{} ",i);
        // }
        // println!("");
        // println!("--------------------");
        is_safe
    }

    pub fn sem_dealloc(&self,thread_id:usize,sem_id:usize){
        let mut inner = self.inner.exclusive_access();
        inner.semaphore_need[thread_id] = None;
        inner.semaphore_finished[thread_id] = true;
        let mut allocation = inner.semaphore_allocation[thread_id].as_mut().unwrap();
        allocation[sem_id] -= 1;
        drop(allocation);
        let mut work = &mut inner.semaphore_work;
        work[sem_id] += 1;
        // for (alloationc,work) in allocation.iter_mut().zip(inner.semaphore_work.iter_mut()){
        //     *work += *alloationc;
        // }
    }
}

impl ProcessControlBlockInner {
    #[allow(unused)]
    pub fn get_user_token(&self) -> usize {
        self.memory_set.token()
    }

    pub fn alloc_fd(&mut self) -> usize {
        if let Some(fd) = (0..self.fd_table.len()).find(|fd| self.fd_table[*fd].is_none()) {
            fd
        } else {
            self.fd_table.push(None);
            self.fd_table.len() - 1
        }
    }

    pub fn alloc_tid(&mut self) -> usize {
        self.task_res_allocator.alloc()
    }

    pub fn dealloc_tid(&mut self, tid: usize) {
        self.task_res_allocator.dealloc(tid)
    }

    pub fn thread_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn get_task(&self, tid: usize) -> Arc<TaskControlBlock> {
        self.tasks[tid].as_ref().unwrap().clone()
    }
}

impl ProcessControlBlock {
    pub fn inner_exclusive_access(&self) -> RefMut<'_, ProcessControlBlockInner> {
        self.inner.exclusive_access()
    }

    // LAB5 HINT: How to initialize deadlock data structures?
    pub fn new(elf_data: &[u8]) -> Arc<Self> {
        // memory_set with elf program headers/trampoline/trap context/user stack
        let (memory_set, ustack_base, entry_point) = MemorySet::from_elf(elf_data);
        // allocate a pid
        let pid_handle = pid_alloc();
        let process = Arc::new(Self {
            pid: pid_handle,
            inner: unsafe {
                UPSafeCell::new(ProcessControlBlockInner {
                    is_zombie: false,
                    memory_set,
                    parent: None,
                    children: Vec::new(),
                    exit_code: 0,
                    fd_table: vec![
                        // 0 -> stdin
                        Some(Arc::new(Stdin)),
                        // 1 -> stdout
                        Some(Arc::new(Stdout)),
                        // 2 -> stderr
                        Some(Arc::new(Stdout)),
                    ],
                    tasks: Vec::new(),
                    task_res_allocator: RecycleAllocator::new(),
                    mutex_list: Vec::new(),
                    semaphore_list: Vec::new(),
                    condvar_list: Vec::new(),
                    detector: None,
                })
            },
        });
        // create a main thread, we should allocate ustack and trap_cx here
        let task = Arc::new(TaskControlBlock::new(
            Arc::clone(&process),
            ustack_base,
            true,
        ));
        // prepare trap_cx of main thread
        let task_inner = task.inner_exclusive_access();
        let trap_cx = task_inner.get_trap_cx();
        let ustack_top = task_inner.res.as_ref().unwrap().ustack_top();
        let kernel_stack_top = task.kernel_stack.get_top();
        drop(task_inner);
        *trap_cx = TrapContext::app_init_context(
            entry_point,
            ustack_top,
            KERNEL_SPACE.exclusive_access().token(),
            kernel_stack_top,
            trap_handler as usize,
        );
        // add main thread to the process
        let mut process_inner = process.inner_exclusive_access();
        process_inner.tasks.push(Some(Arc::clone(&task)));
        drop(process_inner);
        // add main thread to scheduler
        add_task(task);
        process
    }

    // LAB5 HINT: How to initialize deadlock data structures?
    /// Load a new elf to replace the original application address space and start execution
    /// Only support processes with a single thread.
    pub fn exec(self: &Arc<Self>, elf_data: &[u8], args: Vec<String>) {
        assert_eq!(self.inner_exclusive_access().thread_count(), 1);
        // memory_set with elf program headers/trampoline/trap context/user stack
        let (memory_set, ustack_base, entry_point) = MemorySet::from_elf(elf_data);
        let new_token = memory_set.token();
        // substitute memory_set
        self.inner_exclusive_access().memory_set = memory_set;
        // then we alloc user resource for main thread again
        // since memory_set has been changed
        let task = self.inner_exclusive_access().get_task(0);
        let mut task_inner = task.inner_exclusive_access();
        task_inner.res.as_mut().unwrap().ustack_base = ustack_base;
        task_inner.res.as_mut().unwrap().alloc_user_res();
        task_inner.trap_cx_ppn = task_inner.res.as_mut().unwrap().trap_cx_ppn();
        // push arguments on user stack
        let mut user_sp = task_inner.res.as_mut().unwrap().ustack_top();
        user_sp -= (args.len() + 1) * core::mem::size_of::<usize>();
        let argv_base = user_sp;
        let mut argv: Vec<_> = (0..=args.len())
            .map(|arg| {
                translated_refmut(
                    new_token,
                    (argv_base + arg * core::mem::size_of::<usize>()) as *mut usize,
                )
            })
            .collect();
        *argv[args.len()] = 0;
        for i in 0..args.len() {
            user_sp -= args[i].len() + 1;
            *argv[i] = user_sp;
            let mut p = user_sp;
            for c in args[i].as_bytes() {
                *translated_refmut(new_token, p as *mut u8) = *c;
                p += 1;
            }
            *translated_refmut(new_token, p as *mut u8) = 0;
        }
        // make the user_sp aligned to 8B for k210 platform
        user_sp -= user_sp % core::mem::size_of::<usize>();
        // initialize trap_cx
        let mut trap_cx = TrapContext::app_init_context(
            entry_point,
            user_sp,
            KERNEL_SPACE.exclusive_access().token(),
            task.kernel_stack.get_top(),
            trap_handler as usize,
        );
        trap_cx.x[10] = args.len();
        trap_cx.x[11] = argv_base;
        *task_inner.get_trap_cx() = trap_cx;
    }

    // LAB5 HINT: How to initialize deadlock data structures?
    /// Fork from parent to child
    /// Only support processes with a single thread.
    pub fn fork(self: &Arc<Self>) -> Arc<Self> {
        let mut parent = self.inner_exclusive_access();
        assert_eq!(parent.thread_count(), 1);
        // clone parent's memory_set completely including trampoline/ustacks/trap_cxs
        let memory_set = MemorySet::from_existed_user(&parent.memory_set);
        // alloc a pid
        let pid = pid_alloc();
        // copy fd table
        let mut new_fd_table: Vec<Option<Arc<dyn File + Send + Sync>>> = Vec::new();
        for fd in parent.fd_table.iter() {
            if let Some(file) = fd {
                new_fd_table.push(Some(file.clone()));
            } else {
                new_fd_table.push(None);
            }
        }
        // create child process pcb
        let child = Arc::new(Self {
            pid,
            inner: unsafe {
                UPSafeCell::new(ProcessControlBlockInner {
                    is_zombie: false,
                    memory_set,
                    parent: Some(Arc::downgrade(self)),
                    children: Vec::new(),
                    exit_code: 0,
                    fd_table: new_fd_table,
                    tasks: Vec::new(),
                    task_res_allocator: RecycleAllocator::new(),
                    mutex_list: Vec::new(),
                    semaphore_list: Vec::new(),
                    condvar_list: Vec::new(),
                    detector: None,
                })
            },
        });
        // add child
        parent.children.push(Arc::clone(&child));
        // create main thread of child process
        let task = Arc::new(TaskControlBlock::new(
            Arc::clone(&child),
            parent
                .get_task(0)
                .inner_exclusive_access()
                .res
                .as_ref()
                .unwrap()
                .ustack_base(),
            // here we do not allocate trap_cx or ustack again
            // but mention that we allocate a new kernel_stack here
            false,
        ));
        // attach task to child process
        let mut child_inner = child.inner_exclusive_access();
        child_inner.tasks.push(Some(Arc::clone(&task)));
        drop(child_inner);
        // modify kernel_stack_top in trap_cx of this thread
        let task_inner = task.inner_exclusive_access();
        let trap_cx = task_inner.get_trap_cx();
        trap_cx.kernel_sp = task.kernel_stack.get_top();
        drop(task_inner);
        // add this thread to scheduler
        add_task(task);
        child
    }

    pub fn getpid(&self) -> usize {
        self.pid.0
    }

    pub fn kernel_process() -> Arc<Self> {
        let memory_set = MemorySet::kernel_copy();
        let process = Arc::new(ProcessControlBlock {
            pid: super::pid_alloc(),
            inner: unsafe {
                UPSafeCell::new(ProcessControlBlockInner {
                    is_zombie: false,
                    memory_set: memory_set,
                    parent: None,
                    children: Vec::new(),
                    exit_code: 0,
                    fd_table: Vec::new(),
                    tasks: Vec::new(),
                    task_res_allocator: RecycleAllocator::new(),
                    mutex_list: Vec::new(),
                    semaphore_list: Vec::new(),
                    condvar_list: Vec::new(),
                    detector: None,
                })
            },
        });
        process
    }
    pub fn check_mutex(){

    }
}
