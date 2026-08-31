//! PyCapsule bindings for `datafusion-extra-functions`.
//!
//! Exposes each aggregate UDF from the crate through datafusion-python's
//! `__datafusion_aggregate_udf__` protocol, so Python code can do:
//!
//! ```python
//! from datafusion import udaf
//! import datafusion_extra_functions_ffi as ffi
//!
//! mode = udaf(ffi.udaf_by_name("mode"))
//! df.aggregate([], [mode(col("x"))])
//! ```

use std::ffi::CString;
use std::sync::Arc;

use datafusion_expr::AggregateUDF;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(name = "ExtraAggregateUDF", module = "datafusion_extra_functions_ffi")]
pub struct ExtraAggregateUDF {
    inner: Arc<AggregateUDF>,
}

#[pymethods]
impl ExtraAggregateUDF {
    fn name(&self) -> String {
        self.inner.name().to_string()
    }

    fn __repr__(&self) -> String {
        format!("ExtraAggregateUDF({})", self.inner.name())
    }

    fn __datafusion_aggregate_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_aggregate_udf").unwrap();
        let provider = FFI_AggregateUDF::from(Arc::clone(&self.inner));
        PyCapsule::new(py, provider, Some(name))
    }
}

#[pyfunction]
fn list_functions() -> Vec<String> {
    datafusion_extra_functions::all_extra_aggregate_functions()
        .into_iter()
        .map(|f| f.name().to_string())
        .collect()
}

#[pyfunction]
fn udaf_by_name(name: &str) -> PyResult<ExtraAggregateUDF> {
    datafusion_extra_functions::all_extra_aggregate_functions()
        .into_iter()
        .find(|f| f.name() == name)
        .map(|inner| ExtraAggregateUDF { inner })
        .ok_or_else(|| {
            PyKeyError::new_err(format!(
                "No aggregate function named {name:?} in datafusion-extra-functions"
            ))
        })
}

#[pymodule]
fn datafusion_extra_functions_ffi(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ExtraAggregateUDF>()?;
    m.add_function(wrap_pyfunction!(list_functions, m)?)?;
    m.add_function(wrap_pyfunction!(udaf_by_name, m)?)?;
    Ok(())
}
