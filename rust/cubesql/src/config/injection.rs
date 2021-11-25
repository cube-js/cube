use crate::CubeError;
use std::any::{type_name, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
#[allow(deprecated)]
use std::raw::TraitObject;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tokio::sync::RwLock;

pub struct Injector {
    this: Weak<Injector>,
    init_guards: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    services: RwLock<HashMap<String, Arc<dyn DIService>>>,
    factories: RwLock<
        HashMap<
            String,
            Box<
                dyn Fn(Arc<Injector>) -> Pin<Box<dyn Future<Output = Arc<dyn DIService>> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    >,
}

impl Injector {
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            this: this.clone(),
            init_guards: RwLock::new(HashMap::new()),
            services: RwLock::new(HashMap::new()),
            factories: RwLock::new(HashMap::new()),
        })
    }

    pub async fn register_typed_with_default<I: ?Sized, T, F, FF>(&self, factory: FF)
    where
        FF: FnOnce(Arc<Injector>) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = Arc<T>> + Send,
        T: DIService + 'static,
    {
        self.register_typed::<T, T, F, FF>(factory).await;
        self.register_typed::<I, T, _, _>(async move |i: Arc<Injector>| {
            i.get_service_typed::<T>().await
        })
        .await;
    }

    pub async fn register_typed<I: ?Sized, T, F, FF>(&self, factory: FF)
    where
        FF: FnOnce(Arc<Injector>) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = Arc<T>> + Send,
        T: DIService + 'static,
    {
        let name = type_name::<I>().to_string();
        self.factories.write().await.insert(
            name.to_string(),
            Box::new(move |i| {
                let fn_to_move = factory.clone();
                Box::pin(async move {
                    let arc: Arc<dyn DIService> = fn_to_move(i.clone()).await;
                    arc
                })
            }),
        );
        self.init_guards
            .write()
            .await
            .insert(name, Arc::new(Mutex::new(())));
    }

    pub async fn register<F>(
        &self,
        name: &str,
        factory: impl FnOnce(Arc<Injector>) -> F + Send + Sync + Clone + 'static,
    ) where
        F: Future<Output = Arc<dyn DIService>> + Send,
    {
        self.factories.write().await.insert(
            name.to_string(),
            Box::new(move |i| {
                let fn_to_move = factory.clone();
                Box::pin(async move { fn_to_move(i.clone()).await })
            }),
        );
        self.init_guards
            .write()
            .await
            .insert(name.to_string(), Arc::new(Mutex::new(())));
    }
}

impl Injector {
    pub async fn get_service<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> Arc<T> {
        if let Some(s) = self.try_get_service(name).await {
            return s;
        }

        let pending = self
            .init_guards
            .read()
            .await
            .get(name)
            .unwrap_or_else(|| panic!("Service is not found: {}", name))
            .clone();
        // println!("Locking service: {}", name);
        // TODO cycle depends lead to dead lock here
        let _l = pending.lock().await;

        if let Some(s) = self.try_get_service(name).await {
            return s;
        }

        let factories = self.factories.read().await;
        let factory = factories
            .get(name)
            .unwrap_or_else(|| panic!("Service not found: {}", name));
        let service = factory(self.this.upgrade().unwrap()).await;
        // println!("Setting service: {}", name);
        self.services
            .write()
            .await
            .insert(name.to_string(), service.clone());
        service.clone().downcast(service).unwrap()
    }

    pub async fn try_get_service<T: ?Sized + Send + Sync + 'static>(
        &self,
        name: &str,
    ) -> Option<Arc<T>> {
        self.services
            .read()
            .await
            .get(name)
            .map(|s| s.clone().downcast(s.clone()).unwrap())
    }

    pub async fn get_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> Arc<T> {
        self.get_service(type_name::<T>()).await
    }

    pub async fn try_get_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.try_get_service(type_name::<T>()).await
    }

    pub async fn has_service<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> bool {
        self.factories.read().await.contains_key(name)
    }

    pub async fn has_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> bool {
        self.factories.read().await.contains_key(type_name::<T>())
    }
}

pub trait DIService: Send + Sync {
    #[allow(deprecated)]
    fn downcast_ref(
        &self,
        target: TypeId,
        type_name: &'static str,
        arc: Arc<dyn DIService>,
    ) -> Result<TraitObject, CubeError>;
}

impl dyn DIService {
    pub fn downcast<T: ?Sized + 'static>(
        &self,
        arc: Arc<dyn DIService>,
    ) -> Result<Arc<T>, CubeError> {
        unsafe {
            let obj = self.downcast_ref(TypeId::of::<T>(), type_name::<T>(), arc)?;
            #[allow(deprecated)]
            let ptr = *(&obj as *const TraitObject as *const &T);
            Ok(Arc::from_raw(ptr))
        }
    }
}

#[macro_export]
macro_rules! di_service (
    ( $ty: ident, [ $( $trait_ty: ident ),* ]) => {
        #[allow(deprecated)]
        impl $crate::config::injection::DIService for $ty {
            fn downcast_ref(
                &self,
                target: core::any::TypeId,
                type_name: &'static str,
                arc: Arc<dyn $crate::config::injection::DIService>,
            ) -> Result<core::raw::TraitObject, CubeError> {
                unsafe {
                    let ptr = Arc::into_raw(arc);
                    let arc = Arc::<Self>::from_raw(ptr as *const Self);
                    $(
                    if target == core::any::TypeId::of::<dyn $trait_ty>() {
                        let iface_arc: Arc<dyn $trait_ty> = arc;
                        let ptr = Arc::into_raw(iface_arc);
                        return Ok(std::mem::transmute(&*ptr));
                    }
                    )*
                    if target == core::any::TypeId::of::<$ty>() {
                        let typ_arc: Arc<$ty> = arc;
                        let ptr = Arc::into_raw(typ_arc);
                        return Ok(core::raw::TraitObject {
                            data: ptr as *const _ as *mut (),
                            vtable: std::ptr::null_mut(),
                        });
                    }
                }
                Err(CubeError::internal(format!(
                    "Can't cast service to {:?}",
                    type_name
                )))
            }
        }
    }
);
